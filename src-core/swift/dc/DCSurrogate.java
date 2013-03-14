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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
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
import swift.proto.UpdatesNotification;
import swift.pubsub.CommitNotification;
import swift.pubsub.DcPubSubService;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;
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

    DCDataServer dataServer;
    CausalityClock estimatedDCVersion; // estimate of current DC state
    CausalityClock estimatedDCStableVersion; // estimate of current DC state
    static ExecutorService notificationsExecutor;

    DcPubSubService dcPubSub;

    DCSurrogate(int port4Clients, int port4Sequencers, Endpoint sequencerEndpoint, Properties props) {
        this.surrogateId = "s" + System.nanoTime();

        initData(props);

        this.sequencerServerEndpoint = sequencerEndpoint;
        this.cltEndpoint4Sequencer = Networking.rpcConnect().toDefaultService();
        this.srvEndpoint4Clients = Networking.rpcBind(port4Clients).toDefaultService();
        this.srvEndpoint4Sequencer = Networking.rpcBind(port4Sequencers).toDefaultService();

        srvEndpoint4Clients.setHandler(this);
        srvEndpoint4Sequencer.setHandler(this);

        notificationsExecutor = Executors.newFixedThreadPool(DCConstants.SURROGATE_NOTIFIER_THREAD_POOL_SIZE);

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Server ready...");
        }
    }

    private void initData(Properties props) {
        estimatedDCVersion = ClockFactory.newClock();
        estimatedDCStableVersion = ClockFactory.newClock();
        dataServer = new DCDataServer(this, props);

        // HACK HACK
        CausalityClock clk = (CausalityClock) dataServer.dbServer.readSysData("SYS_TABLE", "CURRENT_CLK");
        if (clk != null) {
            System.err.println("SURROGATE CLK:" + clk);
            estimatedDCVersion.merge(clk);
            estimatedDCStableVersion.merge(clk);
        }

        System.err.println("EstimatedDCVersion: " + estimatedDCVersion);
        dcPubSub = new DcPubSubService();
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
    CRDTObject getCRDT(CRDTIdentifier id, CausalityClock clk, String cltId) {
        return dataServer.getCRDT(id, clk, cltId); // call DHT server
    }

    @Override
    public void onReceive(RpcHandle conn, FetchObjectVersionRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectVersionRequest client = " + request.getClientId());
        }

        if (request.hasSubscription())
            getSession(request.getClientId()).subscribe(request.getUid());

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
        final ClientSession session = getSession(request.getClientId());

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

        CRDTObject crdt = getCRDT(request.getUid(), request.getVersion(), request.getClientId());

        if (crdt == null) {
            final CausalityClock versionClock = estimatedDCVersionCopy.clone();
            if (cltLastSeqNo != null) {
                versionClock.recordAllUntil(cltLastSeqNo);
            }
            if (logger.isLoggable(Level.INFO)) {
                logger.info("END FetchObjectVersionRequest not found:" + request.getUid());
            }
            return new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null,
                    versionClock, ClockFactory.newClock(), estimatedDCVersionCopy, estimatedDCStableVersionCopy,
                    request.timestamp, -1, -1);
        } else {

            // CRDTObject<?> commitedCRDT = getCRDT(request.getUid(),
            // request.getSubscriptionType(), request.getClock(),
            // request.getClientId());
            //
            // Set<TimestampMapping> updates =
            // commitedCRDT.crdt.getUpdatesTimestampMappingsSince(request
            // .getDistasterDurableClock());
            //
            // int lat = 0;
            // int mu = updates.size();
            // long now = System.currentTimeMillis();
            //
            // for (TimestampMapping i : updates)
            // lat = (int) Math.max(lat, now -
            // i.getSelectedSystemTimestamp().time());

            synchronized (crdt) {
                crdt.clock.merge(estimatedDCVersionCopy);
                if (cltLastSeqNo != null)
                    crdt.clock.recordAllUntil(cltLastSeqNo);
                final FetchObjectVersionReply.FetchStatus status = (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND
                        : FetchStatus.OK;
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("END FetchObjectVersionRequest clock = " + crdt.clock + "/" + request.getUid());
                }
                return new FetchObjectVersionReply(status, crdt.crdt, crdt.clock, crdt.pruneClock,
                        estimatedDCVersionCopy, estimatedDCStableVersionCopy, request.timestamp, 0, 0);
            }
        }
    }

    @Override
    public void onReceive(final RpcHandle conn, final CommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest client = " + request.getClientId() + ":ts=" + request.getCltTimestamp()
                    + ":nops=" + request.getObjectUpdateGroups().size());
        }
        final ClientSession session = getSession(request.getClientId());
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
        CommitTSReply reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, new CommitTSRequest(txTs, cltTs,
                prvCltTs, estimatedDCVersionCopy, txnOK, ops));

        if (reply == null)
            System.err.println("------------>REPLY FROM SEQUENCER NULL");

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
                CommitNotification notification = new CommitNotification(session.clientId, results, snapshotClock,
                        reply.getCurrVersion(), reply.getStableVersion());

                dcPubSub.publish(notification.uids(), notification);

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
        final ClientSession session = getSession(request.getClientId());
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
    public void onReceive(RpcHandle conn, SeqCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("SeqCommitUpdatesRequest timestamp = " + request.getTimestamp() + ";clt="
                    + request.getCltTimestamp());
        }
        final ClientSession session = getSession("Sequencer");
        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();

        doOneCommit(session, request, snapshotClock);
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
        getSession(request.getClientId()).unsubscribe(request.getUnSubscriptions(), handle);
        handle.enableDeferredReplies(10000);
        handle.reply(new UnsubscribeUpdatesReply(request.getId()));
    }

    class ClientSession extends AbstractPubSub<CRDTIdentifier, CommitNotification> {
        long replyTime;
        String clientId;
        Timestamp lastSeqNo;
        RpcHandle clientHandle;
        AtomicInteger fifoSeqN = new AtomicInteger(0);

        List<CommitNotification> notifications = new ArrayList<CommitNotification>();

        ClientSession(String clientId) {
            this.clientId = clientId;
        }

        Timestamp getLastSeqNo() {
            return lastSeqNo;
        }

        void setLastSeqNo(Timestamp cltTs) {
            this.lastSeqNo = cltTs;
        }

        Task notifier = new Task(0) {
            public void run() {
                if (clientHandle != null) {

                    List<CommitNotification> notificationsCopy;
                    synchronized (notifications) {
                        notificationsCopy = new ArrayList<CommitNotification>(notifications);
                        notifications.clear();
                    }
                    clientHandle.reply(new UpdatesNotification(fifoSeqN.getAndIncrement(), notificationsCopy));
                }
            }
        };

        /**
         * Update client subscriptions
         * 
         */
        synchronized void subscribe(CRDTIdentifier id) {
            dcPubSub.subscribe(id, this);
        }

        /**
         * Update client subscriptions
         * 
         */
        synchronized void unsubscribe(Set<CRDTIdentifier> ids, RpcHandle handle) {
            this.clientHandle = handle;
            dcPubSub.unsubscribe(ids, this);
        }

        @Override
        synchronized public void notify(Set<CRDTIdentifier> keys, CommitNotification data) {
            synchronized (notifications) {
                if (!clientId.equals(data.getClientId())) {
                    notifications.add(data);
                }
            }
            if (!notifier.isScheduled())
                notifier.reSchedule(0.0);
        }
    }

    synchronized public ClientSession getSession(String clientId) {
        ClientSession session = sessions.get(clientId);
        if (session == null) {
            session = new ClientSession(clientId);
            sessions.put(clientId, session);
        }
        return session;
    }

    private Map<String, ClientSession> sessions = new HashMap<String, ClientSession>();
}
