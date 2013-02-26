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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import swift.client.proto.BatchCommitUpdatesReply;
import swift.client.proto.BatchCommitUpdatesRequest;
import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesReply.CommitStatus;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.client.proto.FastRecentUpdatesReply.SubscriptionStatus;
import swift.client.proto.FastRecentUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.KeepaliveReply;
import swift.client.proto.KeepaliveRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.client.proto.RecentUpdatesRequest;
import swift.client.proto.SubscriptionType;
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.dc.proto.CommitTSReply;
import swift.dc.proto.CommitTSRequest;
import swift.dc.proto.GenerateDCTimestampReply;
import swift.dc.proto.GenerateDCTimestampRequest;
import swift.dc.proto.SeqCommitUpdatesRequest;
import swift.dc.pubsub.CommitNotification;
import swift.dc.pubsub.DcPubSubService;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSub;
import sys.scheduler.Task;
import sys.stats.Tally;
import sys.utils.SlidingIntSet;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
class DCSurrogate extends Handler implements swift.client.proto.SwiftServer {
    static Logger logger = Logger.getLogger(DCSurrogate.class.getName());

    public static String siteId;

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

    DCSurrogate(RpcEndpoint srvEndpoint4Clients, RpcEndpoint srvEndpoint4Sequencers, RpcEndpoint cltEndpoint4Sequencer,
            Endpoint sequencerEndpoint, Properties props) {
        this.surrogateId = "s" + System.nanoTime();

        // For handling incoming requests from scouts and sequencers,
        // respectively.
        this.srvEndpoint4Clients = srvEndpoint4Clients.setHandler(this);

        this.srvEndpoint4Sequencer = srvEndpoint4Sequencers.setHandler(this);

        // For performing requests to sequencer. Avoid thread pool contention.
        this.cltEndpoint4Sequencer = cltEndpoint4Sequencer;

        this.sequencerServerEndpoint = sequencerEndpoint;

        initData(props);
        notificationsExecutor = Executors.newFixedThreadPool(DCConstants.SURROGATE_NOTIFIER_THREAD_POOL_SIZE);

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Server ready...");
        }
    }

    private void initData(Properties props) {
        estimatedDCVersion = ClockFactory.newClock();
        estimatedDCStableVersion = ClockFactory.newClock();
        dataServer = new DCDataServer(this, props);

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

    /*
     * void notifyNewUpdates( ObjectSubscriptionInfo notification) {
     * logger.info( "Notify new updates for:" + notification.getId()); }
     */
    // /**
    // * Return null if CRDT does not exist
    // */
    // <V extends CRDT<V>> CRDTData<V> putCRDT(CRDTIdentifier id, CRDT<V> crdt,
    // CausalityClock clk, CausalityClock prune) {
    // return dataServer.putCRDT(id, crdt, clk, prune); // call DHT server
    // }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> ExecCRDTResult execCRDT(CRDTObjectUpdatesGroup<V> grp, CausalityClock snapshotVersion,
            CausalityClock trxVersion, Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs, CausalityClock curDCVersion) {
        return dataServer.execCRDT(grp, snapshotVersion, trxVersion, txTs, cltTs, prvCltTs, curDCVersion); // call
        // DHT
        // server
    }

    /**
     * Return null if CRDT does not exist
     * 
     * @param subscribe
     *            Subscription type
     */
    CRDTObject<?> getCRDT(CRDTIdentifier id, SubscriptionType subscribe, CausalityClock clk, String cltId) {
        return dataServer.getCRDT(id, subscribe, clk, cltId); // call DHT server
    }

    @Override
    public void onReceive(RpcHandle conn, FetchObjectVersionRequest request) {
        long t0 = System.currentTimeMillis();
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectVersionRequest client = " + request.getClientId());
        }
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
        final ScoutSession session = getSession(request.getClientId());

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

        // if( cmp == CMP_CLOCK.CMP_CONCURRENT) {
        // System.err.println(conn.remoteEndpoint() + "@" + siteId +
        // "  CONCURRENT VERSION : vrs asked : " + request.getVersion() +
        // " ; DC version : " + estimatedDCVersionCopy);
        // }

        CausalityClock estimatedDCStableVersionCopy = getEstimatedDCStableVersionCopy();

        CRDTObject<?> crdt = getCRDT(request.getUid(), request.getSubscriptionType(), request.getVersion(),
                request.getClientId());

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

            CRDTObject<?> commitedCRDT = getCRDT(request.getUid(), request.getSubscriptionType(), request.getClock(),
                    request.getClientId());

            Set<TimestampMapping> updates = commitedCRDT.crdt.getUpdatesTimestampMappingsSince(request
                    .getDistasterDurableClock());

            int lat = 0;
            int mu = updates.size();
            long now = System.currentTimeMillis();

            for (TimestampMapping i : updates)
                lat = (int) Math.max(lat, now - i.getSelectedSystemTimestamp().time());

            if (request.getSubscriptionType() != SubscriptionType.NONE)
                session.addToObserving(request.getUid(), false, crdt.crdt.getClock().clone(), crdt.pruneClock.clone());

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
                        estimatedDCVersionCopy, estimatedDCStableVersionCopy, request.timestamp, mu, lat);
            }
        }
    }

    @Override
    public void onReceive(final RpcHandle conn, GenerateTimestampRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("GenerateTimestampRequest client = " + request.getClientId());
        }
        final ScoutSession session = getSession(request.getClientId());
        GenerateTimestampReply reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, request);
        if (reply != null) {
            conn.reply(reply);
        } else {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("GenerateTimestampRequest client = " + request.getClientId() + " failed...");
                System.err.println(request.getClass() + "-> failed...");
            }
        }
    }

    @Override
    public void onReceive(final RpcHandle conn, KeepaliveRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("KeepaliveRequest client = " + request.getClientId());
        }
        final ScoutSession session = getSession(request.getClientId());
        KeepaliveReply reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, request);
        if (reply != null)
            conn.reply(reply);
        else {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("KeepaliveRequest: forwarding reply");
            }
        }
    }

    @Override
    public void onReceive(final RpcHandle conn, final CommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest client = " + request.getClientId() + ":ts="
                    + request.getClientTimestamp() + ":nops=" + request.getObjectUpdateGroups().size());
        }
        final ScoutSession session = getSession(request.getClientId());
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo());
        }

        Timestamp cltTs = session.getLastSeqNo();
        if (cltTs != null && cltTs.getCounter() >= request.getClientTimestamp().getCounter())
            conn.reply(new CommitUpdatesReply(getEstimatedDCVersionCopy()));
        else
            conn.reply(doProcessOneCommit(session, request));
    }

    Tally t = new Tally();
    Tally divergence = new Tally();

    private CommitUpdatesReply doProcessOneCommit(final ScoutSession session, final CommitUpdatesRequest request) {
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
            logger.info("CommitUpdatesRequest: doProcessOneCommit: client = " + request.getClientId() + ":ts="
                    + request.getClientTimestamp() + ":nops=" + request.getObjectUpdateGroups().size());
        }

        GenerateDCTimestampRequest req1 = new GenerateDCTimestampRequest(request.getClientId(),
                request.getClientTimestamp(), request.getObjectUpdateGroups().size() > 0 ? request
                        .getObjectUpdateGroups().get(0).getDependency() : ClockFactory.newClock());

        long t0 = System.currentTimeMillis();
        GenerateDCTimestampReply tsReply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, req1);

        siteId = tsReply.siteId;

        t.add(System.currentTimeMillis() - t0);
        if (t.numberObs() % 999 == 0)
            logger.info(t.report());

        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
        final Timestamp txTs = tsReply.getTimestamp();
        final Timestamp cltTs = request.getClientTimestamp();
        final Timestamp prvCltTs = session.getLastSeqNo();
        for (CRDTObjectUpdatesGroup<?> o : ops) {
            o.addSystemTimestamp(txTs);
        }

        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
        final CausalityClock trxClock = snapshotClock.clone();
        trxClock.record(txTs);
        Iterator<CRDTObjectUpdatesGroup<?>> it = ops.iterator();
        final ExecCRDTResult[] results = new ExecCRDTResult[ops.size()];

        boolean ok = true;
        int pos = 0;
        CausalityClock estimatedDCVersionCopy0 = null;
        synchronized (estimatedDCVersion) {
            estimatedDCVersionCopy0 = estimatedDCVersion.clone();
        }
        final CausalityClock estimatedDCVersionCopy = estimatedDCVersionCopy0;
        while (it.hasNext()) {
            // TODO: must make this concurrent to be fast
            CRDTObjectUpdatesGroup<?> grp = it.next();
            results[pos] = execCRDT(grp, snapshotClock, trxClock, txTs, cltTs, prvCltTs, estimatedDCVersionCopy);
            ok = ok && results[pos].isResult();
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(grp.getDependency());
            }
            pos++;
        }
        final boolean txResult = ok;
        // TODO: handle failure

        session.setLastSeqNo(cltTs);

        CommitTSRequest req2 = new CommitTSRequest(txTs, cltTs, prvCltTs, estimatedDCVersionCopy, ok,
                request.getObjectUpdateGroups());

        CommitTSReply reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, req2);
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
            }

            CausalityClock estimatedDCStableVersionCopy = null;
            synchronized (estimatedDCStableVersion) {
                estimatedDCStableVersion.merge(reply.getStableVersion());
                estimatedDCStableVersionCopy = getEstimatedDCStableVersionCopy();
            }

            if (txResult && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Commit: for publish DC version: SENDING ; on tx:" + txTs);
                }
                CommitUpdatesReply commitReply = new CommitUpdatesReply(reply.getCurrVersion(), txTs);
                CommitNotification notification = new CommitNotification(results, commitReply);

                notification.dependencies = request.getObjectUpdateGroups().get(0).getDependency();
                notification.currVersion = reply.getCurrVersion();
                notification.stableVersion = reply.getStableVersion();

                dcPubSub.publish(notification.uids(), notification);

                return commitReply;
            }
            System.err.println("------------>CRDT EXEC FAILED...");
        }
        return new CommitUpdatesReply();
    }

    @Override
    public void onReceive(RpcHandle conn, BatchCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest client = " + request.getClientId() + ":batch size="
                    + request.getCommitRequests().size());
        }
        final ScoutSession session = getSession(request.getClientId());
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo());
        }
        // for( CRDTObjectUpdatesGroup u: request.getObjectUpdateGroups())
        // u.timeInDC = System.nanoTime();

        List<Timestamp> tsLst = new LinkedList<Timestamp>();
        LinkedList<CommitUpdatesReply> reply = new LinkedList<CommitUpdatesReply>();
        for (CommitUpdatesRequest r : request.getCommitRequests()) {
            if (session.getLastSeqNo() != null
                    && session.getLastSeqNo().getCounter() >= r.getClientTimestamp().getCounter()) {
                reply.addLast(new CommitUpdatesReply(getEstimatedDCVersionCopy()));
            } else {
                r.addTimestampsToDeps(tsLst);
                CommitUpdatesReply repOne = doProcessOneCommit(session, r);
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

    private void updateEstimatedDCVersion() {
        cltEndpoint4Sequencer.send(sequencerServerEndpoint, new LatestKnownClockRequest("surrogate"),
                new LatestKnownClockReplyHandler() {
                    @Override
                    public void onReceive(RpcHandle conn0, LatestKnownClockReply reply) {
                        synchronized (estimatedDCVersion) {
                            estimatedDCVersion.merge(reply.getClock());
                        }
                        synchronized (estimatedDCStableVersion) {
                            estimatedDCStableVersion.merge(reply.getDistasterDurableClock());
                        }
                    }
                });
    }

    @Override
    public void onReceive(final RpcHandle conn, LatestKnownClockRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("LatestKnownClockRequest client = " + request.getClientId());
        }
        final ScoutSession session = getSession(request.getClientId());
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
    public void onReceive(RpcHandle conn, UnsubscribeUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("UnsubscribeUpdatesRequest client = " + request.getClientId());
        }
        final ScoutSession session = getSession(request.getClientId());
        session.remFromObserving(request.getUids());
    }

    @Override
    public void onReceive(RpcHandle conn, RecentUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("RecentUpdatesRequest client = " + request.getClientId());
        }
        final ScoutSession session = getSession(request.getClientId());
        session.dumpNewUpdates(conn, request);
    }

    @Override
    public void onReceive(RpcHandle conn, FastRecentUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FastRecentUpdatesRequest client = " + request.getClientId());
        }
        final ScoutSession session = getSession(request.getClientId());
        session.dumpNewUpdates(conn, request, getEstimatedDCVersionCopy(), getEstimatedDCStableVersionCopy());
    }

    @Override
    public void onReceive(RpcHandle conn, SeqCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("SeqCommitUpdatesRequest timestamp = " + request.getTimestamp() + ";clt="
                    + request.getCltTimestamp());
        }

        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
        final Timestamp ts = request.getTimestamp();
        final Timestamp cltTs = request.getCltTimestamp();
        final Timestamp prvCltTs = request.getPrvCltTimestamp();
        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
        final CausalityClock trxClock = snapshotClock.clone();

        trxClock.record(request.getTimestamp());

        CausalityClock estimatedDCVersionCopy = null;
        synchronized (estimatedDCVersion) {
            estimatedDCVersionCopy = estimatedDCVersion.clone();
        }

        Iterator<CRDTObjectUpdatesGroup<?>> it = ops.iterator();
        final ExecCRDTResult[] results = new ExecCRDTResult[ops.size()];

        boolean ok = true;
        int pos = 0;
        while (it.hasNext()) {
            // TODO: must make this concurrent to be fast
            CRDTObjectUpdatesGroup<?> grp = it.next();
            results[pos] = execCRDT(grp, snapshotClock, trxClock, request.getTimestamp(), request.getCltTimestamp(),
                    request.getPrvCltTimestamp(), estimatedDCVersionCopy);

            ok = ok && results[pos].isResult();
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(grp.getDependency());
            }
            pos++;
        }
        final boolean txResult = ok;
        // TODO: handle failure

        CommitTSReply tsReply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, new CommitTSRequest(ts, cltTs,
                prvCltTs, estimatedDCVersionCopy, ok, request.getObjectUpdateGroups()));

        if (tsReply != null) {

            if (logger.isLoggable(Level.INFO)) {
                logger.info("Commit: received CommitTSRequest");
            }

            CausalityClock estimatedDCVersionCopy2 = null;
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(tsReply.getCurrVersion());
                estimatedDCVersionCopy2 = estimatedDCVersion.clone();
            }
            CausalityClock estimatedDCStableVersionCopy = null;
            synchronized (estimatedDCStableVersion) {
                estimatedDCStableVersion.merge(tsReply.getStableVersion());
                estimatedDCStableVersionCopy = estimatedDCStableVersion.clone();
            }
            if (txResult && tsReply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                // for (int i = 0; i < results.length; i++) {
                // ExecCRDTResult result = results[i];
                // if (result == null)
                // continue;
                // if (result.hasNotification()) {
                // if (results[i].isNotificationOnly()) {
                // dcPubSub.publish(result.getId(), new
                // DHTSendNotification(result.getInfo().cloneNotification(),
                // estimatedDCVersionCopy2, estimatedDCStableVersionCopy));
                // } else {
                // dcPubSub.publish(result.getId(), new
                // DHTSendNotification(result.getInfo(),
                // estimatedDCVersionCopy2, estimatedDCStableVersionCopy));
                // }
                // }
                // }

                CommitUpdatesReply commitReply = new CommitUpdatesReply(tsReply.getCurrVersion(), ts);
                CommitNotification notification = new CommitNotification(results, commitReply);

                dcPubSub.publish(notification.uids(), notification);
            }
        } else {
            Thread.dumpStack();
        }
    }

    class ScoutSession implements PubSub.Handler<CRDTIdentifier, CommitNotification> {
        long replyTime;
        String clientId;
        Timestamp lastSeqNo;
        RpcHandle clientHandle;
        AtomicInteger fifoSeqN = new AtomicInteger(0);

        Set<CRDTIdentifier> subscriptions = new HashSet<CRDTIdentifier>();
        Map<CRDTIdentifier, SlidingIntSet> counters = new HashMap<CRDTIdentifier, SlidingIntSet>();

        List<ObjectSubscriptionInfo> notifications = new ArrayList<ObjectSubscriptionInfo>();

        ScoutSession(String clientId) {
            this.clientId = clientId;
        }

        Timestamp getLastSeqNo() {
            return lastSeqNo;
        }

        void setLastSeqNo(Timestamp cltTs) {
            this.lastSeqNo = cltTs;
        }

        void dumpNewUpdates(RpcHandle conn, RecentUpdatesRequest request) {
            Thread.dumpStack();
        }

        Task notifier = new Task(0) {
            public void run() {

                if (clientHandle != null) {
                    final SubscriptionStatus status = subscriptions.size() == 0 ? SubscriptionStatus.LOST
                            : SubscriptionStatus.ACTIVE;

                    List<ObjectSubscriptionInfo> notificationsCopy;
                    synchronized (notifications) {
                        notificationsCopy = new ArrayList<ObjectSubscriptionInfo>(notifications);
                        notifications.clear();
                        reSchedule(replyTime * 0.001);
                    }
                    clientHandle.reply(new FastRecentUpdatesReply(status, notificationsCopy, estimatedDCVersion,
                            estimatedDCStableVersion, fifoSeqN.getAndIncrement()));

                }
            }
        };

        void dumpNewUpdates(final RpcHandle conn, final FastRecentUpdatesRequest request,
                final CausalityClock estimatedDCVersion, final CausalityClock estimatedDCStableVersion) {

            clientHandle = conn;
            replyTime = request.getMaxBlockingTimeMillis();

            conn.enableDeferredReplies(request.getMaxBlockingTimeMillis() * 10);

            if (notifications.size() > 0)
                notifier.reSchedule(0.0);
        }

        /**
         * Add client to start observing changes on CRDT id
         * 
         * @param observing
         *            fi true, client wants to receive updated; otherwise,
         *            notifications
         */
        synchronized void addToObserving(CRDTIdentifier id, boolean observing, CausalityClock clk,
                CausalityClock pruneClk) {
            subscriptions.add(id);
            dcPubSub.subscribe(id, this);
        }

        /**
         * Removes client from observing changes on CRDT id
         */
        synchronized void remFromObserving(Set<CRDTIdentifier> uids) {
            subscriptions.removeAll(uids);
            counters.keySet().removeAll(uids);
            for (CRDTIdentifier i : uids)
                dcPubSub.unsubscribe(i, this);
        }

        @Override
        public void notify(CRDTIdentifier key, CommitNotification info) {
        }

        @Override
        synchronized public void notify(Set<CRDTIdentifier> keys, CommitNotification data) {
            synchronized (notifications) {
                for (ObjectSubscriptionInfo i : data.info())
                    if (subscriptions.contains(i.getId()))
                        notifications.add(i);

                if (notifications.size() > 0)
                    notifier.reSchedule(0.0);
            }
        }

        synchronized SlidingIntSet getCounter(CRDTIdentifier id) {
            SlidingIntSet res = counters.get(id);
            if (res == null)
                counters.put(id, res = new SlidingIntSet());
            return res;
        }
    }

    synchronized public ScoutSession getSession(String clientId) {
        ScoutSession session = sessions.get(clientId);
        if (session == null) {
            session = new ScoutSession(clientId);
            sessions.put(clientId, session);
        }
        return session;
    }

    private Map<String, ScoutSession> sessions = new HashMap<String, ScoutSession>();

}
