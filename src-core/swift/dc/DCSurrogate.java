/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDT;
import swift.crdt.core.ManagedCRDT;
import swift.proto.BatchCommitUpdatesReply;
import swift.proto.BatchCommitUpdatesRequest;
import swift.proto.CommitTSReply;
import swift.proto.CommitTSRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesReply.CommitStatus;
import swift.proto.CommitUpdatesRequest;
import swift.proto.FetchObjectDeltaRequest;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionReply.FetchStatus;
import swift.proto.FetchObjectVersionRequest;
import swift.proto.GenerateDCTimestampReply;
import swift.proto.GenerateDCTimestampRequest;
import swift.proto.LatestKnownClockReply;
import swift.proto.LatestKnownClockRequest;
import swift.proto.SeqCommitUpdatesRequest;
import swift.proto.SwiftProtocolHandler;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import swift.pubsub.SnapshotNotification;
import swift.pubsub.SurrogatePubSubService;
import swift.pubsub.SwiftSubscriber;
import swift.pubsub.UpdateNotification;
import sys.RpcServices;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.RemoteSubscriber;
//import swift.client.proto.FastRecentUpdatesReply;
//import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
//import swift.client.proto.FastRecentUpdatesReply.SubscriptionStatus;
//import swift.client.proto.FastRecentUpdatesRequest;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
class DCSurrogate extends SwiftProtocolHandler {
    static Logger logger = Logger.getLogger(DCSurrogate.class.getName());

    String surrogateId;
    RpcEndpoint srvEndpoint4Clients;
    RpcEndpoint srvEndpoint4Sequencer;

    Endpoint sequencerServerEndpoint;
    RpcEndpoint cltEndpoint4Sequencer;
    RpcEndpoint endpoint4PubSub;

    DCDataServer dataServer;
    CausalityClock estimatedDCVersion; // estimate of current DC state
    CausalityClock estimatedDCStableVersion; // estimate of current DC state

    SurrogatePubSubService suPubSub;
    ExecutorService executor = Executors.newCachedThreadPool();

    DCSurrogate(int port4Clients, int port4Sequencers, Endpoint sequencerEndpoint, Properties props) {
        this.surrogateId = "s" + System.nanoTime();

        this.sequencerServerEndpoint = sequencerEndpoint;
        this.cltEndpoint4Sequencer = Networking.rpcConnect().toDefaultService();
        this.srvEndpoint4Clients = Networking.rpcBind(port4Clients).toDefaultService();
        this.srvEndpoint4Sequencer = Networking.rpcBind(port4Sequencers).toDefaultService();

        this.endpoint4PubSub = srvEndpoint4Clients.getFactory().toService(RpcServices.PUBSUB.ordinal(), this);

        srvEndpoint4Clients.setHandler(this);
        srvEndpoint4Sequencer.setHandler(this);

        suPubSub = new SurrogatePubSubService(executor, endpoint4PubSub);
        dataServer = new DCDataServer(this, props, endpoint4PubSub, suPubSub);

        initData(props);

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Server ready...");
        }
    }

    private void initData(Properties props) {
        estimatedDCVersion = ClockFactory.newClock();
        estimatedDCStableVersion = ClockFactory.newClock();

        // HACK HACK
        CausalityClock clk = (CausalityClock) dataServer.dbServer.readSysData("SYS_TABLE", "CURRENT_CLK");
        if (clk != null) {
            System.err.println("SURROGATE CLK:" + clk);
            estimatedDCVersion.merge(clk);
            estimatedDCStableVersion.merge(clk);
        }

        System.err.println("EstimatedDCVersion: " + estimatedDCVersion);
    }

    String getId() {
        return surrogateId;
    }

    CausalityClock getEstimatedDCVersionCopy() {
        synchronized (estimatedDCVersion) {
            return estimatedDCVersion.clone();
        }
    }

    CausalityClock getEstimatedDCStableVersionCopy() {
        synchronized (estimatedDCStableVersion) {
            return estimatedDCStableVersion.clone();
        }
    }

    /********************************************************************************************
     * Methods related with notifications from clients
     *******************************************************************************************/

    <V extends CRDT<V>> ExecCRDTResult execCRDT(CRDTObjectUpdatesGroup<V> grp, CausalityClock snapshotVersion,
            CausalityClock trxVersion, Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs, CausalityClock curDCVersion) {

        return dataServer.execCRDT(grp, snapshotVersion, trxVersion, txTs, cltTs, prvCltTs, curDCVersion);
    }

    private void updateEstimatedDCVersion() {
        LatestKnownClockReply reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint,
                new LatestKnownClockRequest("surrogate"));
        if (reply != null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("LatestKnownClockRequest: forwarding reply:" + reply.getClock());
            }
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(reply.getClock());
            }
            synchronized (estimatedDCStableVersion) {
                estimatedDCStableVersion.merge(reply.getDistasterDurableClock());
            }
        }
    }

    /**
     * Return null if CRDT does not exist
     * 
     * @param subscribe
     *            Subscription type
     */
    ManagedCRDT getCRDT(CRDTIdentifier id, CausalityClock clk, String clientId) {
        return dataServer.getCRDT(id, clk, clientId, suPubSub.isSubscribed(id));
    }

    @Override
    public void onReceive(RpcHandle conn, FetchObjectVersionRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectVersionRequest client = " + request.getClientId());
        }

        if (request.hasSubscription())
            getSession(request.getClientId(), conn).subscribe(request.getUid());

        conn.reply(handleFetchVersionRequest(conn, request));
    }

    @Override
    public void onReceive(RpcHandle conn, FetchObjectDeltaRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectDeltaRequest client = " + request.getClientId());
        }
        conn.reply(handleFetchVersionRequest(conn, request));
    }

    private FetchObjectVersionReply handleFetchVersionRequest(RpcHandle conn, FetchObjectVersionRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectVersionRequest client = " + request.getClientId() + "; crdt id = "
                    + request.getUid());
        }
        final ClientSession session = getSession(request.getClientId(), conn);

        final Timestamp cltLastSeqNo = session.getLastSeqNo();

        CMP_CLOCK cmp = CMP_CLOCK.CMP_EQUALS;
        CausalityClock estimatedDCVersionCopy = null;
        synchronized (estimatedDCVersion) {
            cmp = request.getVersion() == null ? CMP_CLOCK.CMP_EQUALS : estimatedDCVersion.compareTo(request
                    .getVersion());
            estimatedDCVersionCopy = estimatedDCVersion.clone();
        }
        if (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) {
            updateEstimatedDCVersion();
            synchronized (estimatedDCVersion) {
                cmp = estimatedDCVersion.compareTo(request.getVersion());
                estimatedDCVersionCopy = estimatedDCVersion.clone();
            }
        }

        CausalityClock estimatedDCStableVersionCopy = getEstimatedDCStableVersionCopy();

        ManagedCRDT crdt = getCRDT(request.getUid(), request.getVersion(), request.getClientId());

        if (crdt == null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("END FetchObjectVersionRequest not found:" + request.getUid());
            }
            // if (cltLastSeqNo != null)
            // crdt.augmentWithScoutClock(cltLastSeqNo);
            return new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null,
                    estimatedDCVersionCopy, estimatedDCStableVersionCopy, null);
        } else {

            // for evaluating stale reads
            // Map<String, Object> staleReadsInfo = evaluateStaleReads(request,
            // estimatedDCVersionCopy,
            // estimatedDCStableVersionCopy);

            synchronized (crdt) {
                crdt.augmentWithDCClockWithoutMappings(estimatedDCVersionCopy);
                if (cltLastSeqNo != null)
                    crdt.augmentWithScoutClockWithoutMappings(cltLastSeqNo);
                final FetchObjectVersionReply.FetchStatus status = (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND
                        : FetchStatus.OK;
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("END FetchObjectVersionRequest clock = " + crdt.getClock() + "/" + request.getUid());
                }
                return new FetchObjectVersionReply(status, crdt, estimatedDCVersionCopy, estimatedDCStableVersionCopy,
                        null);
            }
        }
    }

    @Override
    public void onReceive(final RpcHandle conn, final CommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest client = " + request.getClientId() + ":ts=" + request.getCltTimestamp()
                    + ":nops=" + request.getObjectUpdateGroups().size());
        }
        final ClientSession session = getSession(request.getClientId(), conn);
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo());
        }

        Timestamp cltTs = session.getLastSeqNo();
        if (cltTs != null && cltTs.getCounter() >= request.getCltTimestamp().getCounter())
            conn.reply(new CommitUpdatesReply(getEstimatedDCVersionCopy()));
        else
            conn.reply(prepareAndDoCommit(session, request));
    }

    private CommitUpdatesReply prepareAndDoCommit(final ClientSession session, final CommitUpdatesRequest req) {
        final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();
        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();

        GenerateDCTimestampReply tsReply = cltEndpoint4Sequencer.request(sequencerServerEndpoint,
                new GenerateDCTimestampRequest(req.getClientId(), req.getCltTimestamp(), snapshotClock));

        req.setTimestamp(tsReply.getTimestamp());

        // req.setDisasterSafe(); // FOR SOSP EVALUATION...

        return doOneCommit(session, req, snapshotClock);
    }

    private CommitUpdatesReply doOneCommit(final ClientSession session, final CommitUpdatesRequest req,
            final CausalityClock snapshotClock) {
        // 0) updates.addSystemTimestamp(timestampService.allocateTimestamp())
        // 1) let int clientTxs =
        // clientTxClockService.getAndLockNumberOfCommitedTxs(clientId)
        // 2) for all modified objects:
        // crdt.augumentWithScoutClock(new Timestamp(clientId, clientTxs)) //
        // ensures that execute() has enough information to ensure tx
        // idempotence
        // crdt.execute(updates...)
        // crdt.discardScoutClock(clientId) // critical to not polute all data
        // nodes and objects with big vectors, unless we want to do it until
        // pruning
        // 3) clientTxClockService.unlock(clientId)

        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest: doProcessOneCommit: client = " + req.getClientId() + ":ts="
                    + req.getCltTimestamp() + ":nops=" + req.getObjectUpdateGroups().size());
        }

        final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();

        final Timestamp txTs = req.getTimestamp();
        final Timestamp cltTs = req.getCltTimestamp();
        final Timestamp prvCltTs = session.getLastSeqNo();

        for (CRDTObjectUpdatesGroup<?> o : ops)
            o.addSystemTimestamp(txTs);

        final CausalityClock trxClock = snapshotClock.clone();
        trxClock.record(txTs);

        final CausalityClock estimatedDCVersionCopy;
        synchronized (estimatedDCVersion) {
            estimatedDCVersionCopy = estimatedDCVersion.clone();
        }

        int pos = 0;
        boolean txnOK = true;
        final ExecCRDTResult[] results = new ExecCRDTResult[ops.size()];
        for (CRDTObjectUpdatesGroup<?> i : ops) {
            // TODO: must make this concurrent to be fast
            results[pos] = execCRDT(i, snapshotClock, trxClock, txTs, cltTs, prvCltTs, estimatedDCVersionCopy);
            txnOK = txnOK && results[pos].isResult();
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(i.getDependency());
            }
            pos++;
        }

        // TODO: handle failure
        session.setLastSeqNo(cltTs);
        CommitTSReply reply;
        do {
            reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, new CommitTSRequest(txTs, cltTs, prvCltTs,
                    estimatedDCVersionCopy, txnOK, ops, req.disasterSafe(), session.clientId));

            if (reply == null)
                System.err.printf("------------>REPLY FROM SEQUENCER NULL for: %s, who:%s\n", txTs, session.clientId);

        } while (reply == null);

        if (reply != null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Commit: received CommitTSRequest:old vrs:" + estimatedDCVersionCopy + "; new vrs="
                        + reply.getCurrVersion() + ";ts = " + txTs + ";cltts = " + cltTs);
            }
            estimatedDCVersionCopy.record(txTs);
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(reply.getCurrVersion());
                dataServer.dbServer.writeSysData("SYS_TABLE", "CURRENT_CLK", estimatedDCVersion);
            }
            synchronized (estimatedDCStableVersion) {
                estimatedDCStableVersion.merge(reply.getStableVersion());
                dataServer.dbServer.writeSysData("SYS_TABLE", "STABLE_CLK", estimatedDCVersion);
            }

            if (txnOK && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Commit: for publish DC version: SENDING ; on tx:" + txTs);
                }
                CommitUpdatesReply commitReply = new CommitUpdatesReply(reply.getCurrVersion(), txTs);

                Set<CRDTIdentifier> uids = new HashSet<CRDTIdentifier>();
                for (CRDTObjectUpdatesGroup<?> i : ops)
                    uids.add(i.getTargetUID());
                suPubSub.publish(new SnapshotNotification(req.getClientId(), uids, txTs, reply.getCurrVersion()));

                return commitReply;
            }
        }
        return new CommitUpdatesReply();
    }

    @Override
    public void onReceive(RpcHandle conn, BatchCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest client = " + request.getClientId() + ":batch size="
                    + request.getCommitRequests().size());
        }
        final ClientSession session = getSession(request.getClientId(), conn);
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo());
        }

        List<Timestamp> tsLst = new LinkedList<Timestamp>();
        LinkedList<CommitUpdatesReply> reply = new LinkedList<CommitUpdatesReply>();
        for (CommitUpdatesRequest r : request.getCommitRequests()) {
            if (session.getLastSeqNo() != null
                    && session.getLastSeqNo().getCounter() >= r.getCltTimestamp().getCounter()) {
                reply.addLast(new CommitUpdatesReply(getEstimatedDCVersionCopy()));
            } else {
                r.addTimestampsToDeps(tsLst);
                CommitUpdatesReply repOne = prepareAndDoCommit(session, r);

                if (repOne.getStatus() == CommitStatus.COMMITTED_WITH_KNOWN_TIMESTAMPS) {
                    List<Timestamp> tsLstOne = repOne.getCommitTimestamps();
                    if (tsLstOne != null)
                        tsLst.addAll(tsLstOne);
                }
                reply.addLast(repOne);
            }
        }
        conn.reply(new BatchCommitUpdatesReply(reply));
    }

    @Override
    public void onReceive(RpcHandle conn, final SeqCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("SeqCommitUpdatesRequest timestamp = " + request.getTimestamp() + ";clt="
                    + request.getCltTimestamp());
        }
        executor.execute(new Runnable() {
            public void run() {
                ClientSession session = getSession("Sequencer", null);
                List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
                CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
                doOneCommit(session, request, snapshotClock);
            }
        });
    }

    @Override
    public void onReceive(final RpcHandle conn, LatestKnownClockRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("LatestKnownClockRequest client = " + request.getClientId());
        }
        LatestKnownClockReply reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, request);
        if (reply != null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("LatestKnownClockRequest: forwarding reply:" + reply.getClock());
            }
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(reply.getClock());
            }
            synchronized (estimatedDCStableVersion) {
                estimatedDCStableVersion.merge(reply.getDistasterDurableClock());
            }
            conn.reply(reply);
        }
    }

    @Override
    public void onReceive(RpcHandle handle, UnsubscribeUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("UnsubscribeUpdatesRequest client = " + request.getClientId());
        }
        getSession(request.getClientId(), handle).unsubscribe(request.getUnSubscriptions());
        handle.reply(new UnsubscribeUpdatesReply(request.getId()));
    }

    class ClientSession extends RemoteSubscriber implements SwiftSubscriber {
        final String clientId;
        Timestamp lastSeqNo;

        ClientSession(String clientId, RpcHandle handle) {
            super(clientId, endpoint4PubSub, handle.remoteEndpoint());
            this.clientId = clientId;
        }

        Timestamp getLastSeqNo() {
            return lastSeqNo;
        }

        void setLastSeqNo(Timestamp cltTs) {
            this.lastSeqNo = cltTs;
        }

        void subscribe(CRDTIdentifier key) {
            suPubSub.subscribe(key, this);
        }

        void unsubscribe(Set<CRDTIdentifier> keys) {
            suPubSub.unsubscribe(keys, this);
        }

        @Override
        public void onNotification(UpdateNotification update) {
            super.onNotification(update);
        }

        @Override
        public void onNotification(SnapshotNotification snapshot) {
            super.onNotification(snapshot);
        }
    }

    synchronized public ClientSession getSession(String clientId, RpcHandle handle) {
        ClientSession session = sessions.get(clientId);
        if (session == null) {
            session = new ClientSession(clientId, handle);
            sessions.put(clientId, session);
        }
        return session;
    }

    private Map<String, ClientSession> sessions = new HashMap<String, ClientSession>();

    Map<String, Object> evaluateStaleReads(FetchObjectVersionRequest request, CausalityClock estimatedDCVersionCopy,
            CausalityClock estimatedDCStableVersionCopy) {

        Map<String, Object> res = new HashMap<String, Object>();

        ManagedCRDT mostRecentDCVersion = getCRDT(request.getUid(), estimatedDCVersionCopy, request.getClientId());

        ManagedCRDT mostRecentScoutVersion = getCRDT(request.getUid(), request.getClock(), request.getClientId());

        if (mostRecentDCVersion != null && mostRecentScoutVersion != null) {

            List<TimestampMapping> diff1 = mostRecentScoutVersion.getUpdatesTimestampMappingsSince(request
                    .getDistasterDurableClock());

            List<TimestampMapping> diff2 = mostRecentDCVersion.getUpdatesTimestampMappingsSince(request
                    .getDistasterDurableClock());

            List<TimestampMapping> diff3 = mostRecentDCVersion
                    .getUpdatesTimestampMappingsSince(estimatedDCStableVersionCopy);

            res.put("timestamp", request.timestamp);
            res.put("Diff1-scout-normal-vs-scout-stable", diff1.size());
            res.put("Diff2-dc-normal-vs-scout-stable", diff2.size());
            res.put("Diff3-dc-normal-vs-dc-stable", diff3.size());
            // res.put("dc-estimatedVersion", estimatedDCVersionCopy);
            // res.put("dc-estimatedStableVersion",
            // estimatedDCStableVersionCopy);
            // res.put("scout-version", request.getVersion());
            // res.put("scout-clock", request.getClock());
            // res.put("scout-stableClock", request.getDistasterDurableClock());
            // res.put("crdt", mostRecentDCVersion.clock);
        }
        return res;
    }
}
