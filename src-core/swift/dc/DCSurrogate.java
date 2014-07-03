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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.crdt.AbstractLWWRegisterCRDT;
import swift.crdt.LWWStringMapRegisterUpdate;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.proto.BatchCommitUpdatesReply;
import swift.proto.BatchCommitUpdatesRequest;
import swift.proto.ClientRequest;
import swift.proto.CommitTSReply;
import swift.proto.CommitTSRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesReply.CommitStatus;
import swift.proto.CommitUpdatesRequest;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionReply.FetchStatus;
import swift.proto.FetchObjectVersionRequest;
import swift.proto.GenerateDCTimestampReply;
import swift.proto.GenerateDCTimestampRequest;
import swift.proto.LatestKnownClockReply;
import swift.proto.LatestKnownClockRequest;
import swift.proto.PingReply;
import swift.proto.PingRequest;
import swift.proto.SeqCommitUpdatesRequest;
import swift.proto.SwiftProtocolHandler;
import swift.pubsub.BatchUpdatesNotification;
import swift.pubsub.RemoteSwiftSubscriber;
import swift.pubsub.SurrogatePubSubService;
import swift.pubsub.SwiftNotification;
import swift.pubsub.SwiftSubscriber;
import swift.pubsub.UpdateNotification;
import swift.utils.FutureResultHandler;
import sys.Sys;
import sys.dht.DHT_Node;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.PeriodicTask;
//import swift.client.proto.FastRecentUpdatesReply;
//import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
//import swift.client.proto.FastRecentUpdatesReply.SubscriptionStatus;
//import swift.client.proto.FastRecentUpdatesRequest;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
final public class DCSurrogate extends SwiftProtocolHandler {
    public static final boolean FAKE_PRACTI_DEPOT_VECTORS = false;
    public static final boolean OPTIMIZED_VECTORS_IN_BATCH = false;
    static Logger logger = Logger.getLogger(DCSurrogate.class.getName());

    String siteId;
    String surrogateId;
    RpcEndpoint srvEndpoint4Clients;
    RpcEndpoint srvEndpoint4Sequencer;

    Endpoint sequencerServerEndpoint;
    RpcEndpoint cltEndpoint4Sequencer;

    DCDataServer dataServer;
    CausalityClock estimatedDCVersion; // estimate of current DC state
    CausalityClock estimatedDCStableVersion; // estimate of current DC state

    AtomicReference<CausalityClock> estimatedDCVersionShadow; // read only
                                                              // estimate of
                                                              // current DC
                                                              // state
    AtomicReference<CausalityClock> estimatedDCStableVersionShadow; // read only
                                                                    // estimate
                                                                    // of
                                                                    // current
                                                                    // DC
    // state

    public SurrogatePubSubService suPubSub;

    ThreadPoolExecutor crdtExecutor;
    Executor generalExecutor = Executors.newCachedThreadPool();

    DCSurrogate(String siteId, int port4Clients, int port4Sequencers, Endpoint sequencerEndpoint, Properties props) {
        this.siteId = siteId;
        this.surrogateId = "s" + System.nanoTime();

        this.sequencerServerEndpoint = sequencerEndpoint;
        this.cltEndpoint4Sequencer = Networking.rpcConnect().toDefaultService();
        this.srvEndpoint4Clients = Networking.rpcBind(port4Clients).toDefaultService();
        this.srvEndpoint4Sequencer = Networking.rpcBind(port4Sequencers).toDefaultService();

        srvEndpoint4Clients.setHandler(this);
        srvEndpoint4Sequencer.setHandler(this);

        srvEndpoint4Clients.getFactory().setExecutor(Executors.newCachedThreadPool());
        srvEndpoint4Sequencer.getFactory().setExecutor(Executors.newFixedThreadPool(2));

        suPubSub = new SurrogatePubSubService(generalExecutor, this);
        dataServer = new DCDataServer(this, props, suPubSub);

        ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(512);
        crdtExecutor = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, workQueue);
        crdtExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        initData(props);

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Server ready...");
        }

        DHT_Node.init(siteId, "surrogates", srvEndpoint4Clients.localEndpoint());
        new PeriodicTask(0.0, 0.1) {
            public void run() {
                updateEstimatedDCVersion();
            }
        };
    }

    private void initData(Properties props) {
        estimatedDCVersion = ClockFactory.newClock();
        estimatedDCStableVersion = ClockFactory.newClock();

        // HACK HACK
        CausalityClock clk = (CausalityClock) dataServer.dbServer.readSysData("SYS_TABLE", "CURRENT_CLK");
        if (clk != null) {
            logger.info("SURROGATE CLK:" + clk);
            estimatedDCVersion.merge(clk);
            estimatedDCStableVersion.merge(clk);
        }

        estimatedDCVersionShadow = new AtomicReference<CausalityClock>(estimatedDCVersion.clone());
        estimatedDCStableVersionShadow = new AtomicReference<CausalityClock>(estimatedDCStableVersion.clone());

        logger.info("EstimatedDCVersion: " + estimatedDCVersion);
    }

    public String getId() {
        return surrogateId;
    }

    public CausalityClock getEstimatedDCVersionCopy() {
        return estimatedDCVersionShadow.get().clone();
    }

    CausalityClock getEstimatedDCStableVersionCopy() {
        return estimatedDCStableVersionShadow.get().clone();
    }

    public void updateEstimatedDCVersion(CausalityClock cc) {
        synchronized (estimatedDCVersion) {
            estimatedDCVersion.merge(cc);
            estimatedDCVersionShadow.set(estimatedDCVersion.clone());
        }
    }

    public void updateEstimatedDCStableVersion(CausalityClock cc) {
        synchronized (estimatedDCStableVersion) {
            estimatedDCStableVersion.merge(cc);
            estimatedDCStableVersionShadow.set(estimatedDCStableVersion.clone());
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
        cltEndpoint4Sequencer.send(sequencerServerEndpoint, new LatestKnownClockRequest("surrogate", false), this, 0);
    }

    public void onReceive(RpcHandle conn, LatestKnownClockReply reply) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("LatestKnownClockReply: clk:" + reply.getClock());
        }

        updateEstimatedDCVersion(reply.getClock());
        updateEstimatedDCStableVersion(reply.getDistasterDurableClock());
    }

    public void onReceive(RpcHandle conn, FetchObjectVersionRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectVersionRequest client = " + request.getClientId());
        }
        // LWWStringMapRegisterCRDT initialCheckpoint = new
        // LWWStringMapRegisterCRDT(request.getUid());
        // FAKE_INIT_UPDATE.applyTo(initialCheckpoint);
        // conn.reply(new FetchObjectVersionReply(FetchStatus.OK, new
        // ManagedCRDT<LWWStringMapRegisterCRDT>(request
        // .getUid(), initialCheckpoint, request.getVersion(), true),
        // request.getVersion(), request.getVersion()));
        // return;
        // conn.reply(new FetchObjectVersionReply(FetchStatus.OK, new
        // ManagedCRDT<PutOnlyLWWStringMapCRDT>(request
        // .getUid(), new PutOnlyLWWStringMapCRDT(request.getUid()),
        // request.getVersion(), true), request
        // .getVersion(), request.getVersion()));
        // return;

        if (request.hasSubscription())
            getSession(request).subscribe(request.getUid());

        request.setHandle(conn);
        handleFetchVersionRequest(conn, request);
    }

    @SuppressWarnings("rawtypes")
    private void handleFetchVersionRequest(RpcHandle conn, final FetchObjectVersionRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("FetchObjectVersionRequest client = " + request.getClientId() + "; crdt id = "
                    + request.getUid());
        }
        final ClientSession session = getSession(request);

        final Timestamp cltLastSeqNo = session.getLastSeqNo();

        CMP_CLOCK cmp = CMP_CLOCK.CMP_EQUALS;
        CausalityClock estimatedDCVersionCopy = getEstimatedDCVersionCopy();
        cmp = request.getVersion() == null ? CMP_CLOCK.CMP_EQUALS : estimatedDCVersionCopy.compareTo(request
                .getVersion());

        if (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) {
            updateEstimatedDCVersion();
            estimatedDCVersionCopy = getEstimatedDCVersionCopy();
            cmp = estimatedDCVersionCopy.compareTo(request.getVersion());
        }

        final CMP_CLOCK finalCmpClk = cmp;
        final CausalityClock finalEstimatedDCVersionCopy = estimatedDCVersionCopy;

        CausalityClock estimatedDCStableVersionCopy = getEstimatedDCStableVersionCopy();

        final CausalityClock disasterSafeVVReply = request.isSendDCVector() ? estimatedDCStableVersionCopy.clone()
                : null;
        if (disasterSafeVVReply != null) {
            disasterSafeVVReply.intersect(estimatedDCVersionCopy);
        }

        // TODO: for nodes !request.isDisasterSafe() send it
        // less
        // frequently (it's for pruning only)
        final CausalityClock vvReply = !request.isDisasterSafeSession() && request.isSendDCVector() ? estimatedDCVersionCopy
                .clone() : null;

        dataServer.getCRDT(request.getUid(), request.getVersion(), request.getClientId(), request.hasSubscription(),
                new FutureResultHandler<ManagedCRDT>() {
                    @Override
                    public void onResult(ManagedCRDT crdt) {

                        if (crdt == null) {
                            if (logger.isLoggable(Level.INFO)) {
                                logger.info("END FetchObjectVersionRequest not found:" + request.getUid());
                            }
                            request.replyHandle.reply(new FetchObjectVersionReply(
                                    FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null, vvReply,
                                    disasterSafeVVReply));
                        } else {
                            synchronized (crdt) {
                                crdt.augmentWithDCClockWithoutMappings(finalEstimatedDCVersionCopy);

                                if (cltLastSeqNo != null)
                                    crdt.augmentWithScoutClockWithoutMappings(cltLastSeqNo);
                                final FetchObjectVersionReply.FetchStatus status = (finalCmpClk == CMP_CLOCK.CMP_ISDOMINATED || finalCmpClk == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND
                                        : FetchStatus.OK;
                                if (status == FetchStatus.VERSION_NOT_FOUND) {
                                    logger.warning("Requested version " + request.getVersion() + " of object "
                                            + request.getUid() + " not available; local version: "
                                            + finalEstimatedDCVersionCopy);
                                }
                                if (logger.isLoggable(Level.INFO)) {
                                    logger.info("END FetchObjectVersionRequest clock = " + crdt.getClock() + "/"
                                            + request.getUid());
                                }
                                request.replyHandle.reply(new FetchObjectVersionReply(status, crdt, vvReply,
                                        disasterSafeVVReply));
                            }
                        }

                    }
                });
    }

    @Override
    public void onReceive(final RpcHandle conn, final CommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("CommitUpdatesRequest client = " + request.getClientId() + ":ts=" + request.getCltTimestamp()
                    + ":nops=" + request.getObjectUpdateGroups().size());
        }
        final ClientSession session = getSession(request);
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
        final CausalityClock dependenciesClock = ops.size() > 0 ? req.getDependencyClock() : ClockFactory.newClock();

        GenerateDCTimestampReply tsReply = cltEndpoint4Sequencer.request(sequencerServerEndpoint,
                new GenerateDCTimestampRequest(req.getClientId(), req.isDisasterSafeSession(), req.getCltTimestamp(),
                        dependenciesClock));

        req.setTimestamp(tsReply.getTimestamp());

        // req.setDisasterSafe(); // FOR SOSP EVALUATION...

        return doOneCommit(session, req, dependenciesClock);
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

        final CausalityClock estimatedDCVersionCopy = getEstimatedDCVersionCopy();

        int pos = 0;
        final AtomicBoolean txnOK = new AtomicBoolean(true);
        final AtomicReferenceArray<ExecCRDTResult> results = new AtomicReferenceArray<ExecCRDTResult>(ops.size());

        if (ops.size() > 2) { // do multiple execCRDTs in parallel
            final Semaphore s = new Semaphore(0);
            for (final CRDTObjectUpdatesGroup<?> i : ops) {
                final int j = pos++;
                crdtExecutor.execute(new Runnable() {
                    public void run() {
                        try {
                            results.set(j,
                                    execCRDT(i, snapshotClock, trxClock, txTs, cltTs, prvCltTs, estimatedDCVersionCopy));
                            txnOK.compareAndSet(true, results.get(j).isResult());
                            updateEstimatedDCVersion(i.getDependency());
                        } finally {
                            s.release();
                        }
                    }
                });
            }
            s.acquireUninterruptibly(ops.size());
        } else {
            for (final CRDTObjectUpdatesGroup<?> i : ops) {
                results.set(pos, execCRDT(i, snapshotClock, trxClock, txTs, cltTs, prvCltTs, estimatedDCVersionCopy));
                txnOK.compareAndSet(true, results.get(pos).isResult());
                updateEstimatedDCVersion(i.getDependency());
                pos++;
            }
        }

        // TODO: handle failure
        session.setLastSeqNo(cltTs);
        CommitTSReply reply;
        do {
            reply = cltEndpoint4Sequencer.request(sequencerServerEndpoint, new CommitTSRequest(txTs, cltTs, prvCltTs,
                    estimatedDCVersionCopy, txnOK.get(), ops, req.disasterSafe(), session.clientId));

            if (reply == null)
                logger.severe(String.format("------------>REPLY FROM SEQUENCER NULL for: %s, who:%s\n", txTs,
                        session.clientId));

        } while (reply == null);

        if (reply != null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Commit: received CommitTSRequest:old vrs:" + estimatedDCVersionCopy + "; new vrs="
                        + reply.getCurrVersion() + ";ts = " + txTs + ";cltts = " + cltTs);
            }
            estimatedDCVersionCopy.record(txTs);
            updateEstimatedDCVersion(reply.getCurrVersion());
            dataServer.dbServer.writeSysData("SYS_TABLE", "CURRENT_CLK", getEstimatedDCVersionCopy());

            updateEstimatedDCStableVersion(reply.getStableVersion());
            dataServer.dbServer.writeSysData("SYS_TABLE", "STABLE_CLK", getEstimatedDCStableVersionCopy());

            if (txnOK.get() && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Commit: for publish DC version: SENDING ; on tx:" + txTs);
                }
                // Set<CRDTIdentifier> uids = new HashSet<CRDTIdentifier>();
                // for (CRDTObjectUpdatesGroup<?> i : ops)
                // uids.add(i.getTargetUID());
                //
                // suPubSub.publish(new SnapshotNotification(req.getClientId(),
                // uids, txTs, reply.getCurrVersion()));

                return new CommitUpdatesReply(txTs);
            }
        }
        return new CommitUpdatesReply();
    }

    @Override
    public void onReceive(RpcHandle conn, BatchCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("BatchCommitUpdatesRequest client = " + request.getClientId() + ":batch size="
                    + request.getCommitRequests().size());
        }
        final ClientSession session = getSession(request);
        if (logger.isLoggable(Level.INFO)) {
            logger.info("BatchCommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo());
        }
        List<Timestamp> tsLst = new LinkedList<Timestamp>();
        LinkedList<CommitUpdatesReply> reply = new LinkedList<CommitUpdatesReply>();
        for (CommitUpdatesRequest r : request.getCommitRequests()) {
            if (session.getLastSeqNo() != null
                    && session.getLastSeqNo().getCounter() >= r.getCltTimestamp().getCounter()) {
                reply.addLast(new CommitUpdatesReply(getEstimatedDCVersionCopy()));
                // FIXME: unless the timestamp is stable andpruned, we need to
                // send a precise mapping to the client!
                // Also, non-stable clock should appear in internal dependencies
                // in the batch.
            } else {
                // Respect internal dependencies in the batch.
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
        generalExecutor.execute(new Runnable() {
            public void run() {
                ClientSession session = getSession(request.getCltTimestamp().getIdentifier(), false);
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

        cltEndpoint4Sequencer.send(sequencerServerEndpoint, request, new SwiftProtocolHandler() {
            public void onReceive(RpcHandle conn2, LatestKnownClockReply reply) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("LatestKnownClockRequest: forwarding reply:" + reply.getClock());
                }
                updateEstimatedDCVersion(reply.getClock());
                updateEstimatedDCStableVersion(reply.getDistasterDurableClock());

                conn.reply(reply);
            }
        }, 0);
    }

    @Override
    public void onReceive(final RpcHandle conn, PingRequest request) {
        PingReply reply = new PingReply(request.getTimeAtSender(), System.currentTimeMillis());
        conn.reply(reply);
    }

    public class ClientSession extends RemoteSwiftSubscriber implements SwiftSubscriber {

        final String clientId;
        Timestamp lastSeqNo;
        boolean disasterSafe;
        private CausalityClock clientFakeVectorKnowledge;
        private CausalityClock lastSnapshotVector;

        ClientSession(String clientId, boolean disasterSafe) {
            super(clientId, suPubSub.endpoint());
            this.clientId = clientId;
            this.disasterSafe = disasterSafe;
            if (FAKE_PRACTI_DEPOT_VECTORS) {
                clientFakeVectorKnowledge = ClockFactory.newClock();
            }
            if (OPTIMIZED_VECTORS_IN_BATCH) {
                lastSnapshotVector = ClockFactory.newClock();
            }

            new PeriodicTask(0.0, DCConstants.NOTIFICATION_PERIOD * 0.001) {
                public void run() {
                    tryFireClientNotification();
                }
            };
        }

        public synchronized CausalityClock getMinVV() {
            return suPubSub.minDcVersion();
        }

        public ClientSession setClientEndpoint(Endpoint remote) {
            super.setRemote(remote);
            return this;
        }

        Timestamp getLastSeqNo() {
            return lastSeqNo;
        }

        void setLastSeqNo(Timestamp cltTs) {
            this.lastSeqNo = cltTs;
        }

        public void subscribe(CRDTIdentifier key) {
            suPubSub.subscribe(key, this);
        }

        public void unsubscribe(Set<CRDTIdentifier> keys) {
            suPubSub.unsubscribe(keys, this);
        }

        long lastNotification = 0L;

        List<CRDTObjectUpdatesGroup<?>> pending = new ArrayList<CRDTObjectUpdatesGroup<?>>();

        synchronized public void onNotification(final UpdateNotification update) {

            if (update.srcId.equals(clientId)) {
                // Ignore
                return;
            }

            pending.addAll(update.info.getUpdates());

            tryFireClientNotification();
        }

        protected synchronized CausalityClock tryFireClientNotification() {
            long now = Sys.Sys.timeMillis();
            if (now <= (lastNotification + DCConstants.NOTIFICATION_PERIOD)) {
                return null;
            }

            final CausalityClock snapshot = suPubSub.minDcVersion();
            if (disasterSafe) {
                snapshot.intersect(getEstimatedDCStableVersionCopy());
            }

            if (OPTIMIZED_VECTORS_IN_BATCH) {
                for (final String dcId : lastSnapshotVector.getSiteIds()) {
                    if (lastSnapshotVector.includes(snapshot.getLatest(dcId))) {
                        snapshot.drop(dcId);
                    }
                }
                lastSnapshotVector.merge(snapshot);
            }

            final HashMap<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> objectsUpdates = new HashMap<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>>();
            final Iterator<CRDTObjectUpdatesGroup<?>> iter = pending.iterator();
            while (iter.hasNext()) {
                final CRDTObjectUpdatesGroup<?> u = iter.next();
                if (u.anyTimestampIncluded(snapshot)) {
                    // FIXME: for at-most-once check if any timestamp is
                    // included in the previous clock of the scout
                    List<CRDTObjectUpdatesGroup<?>> objectUpdates = objectsUpdates.get(u.getTargetUID());
                    if (objectUpdates == null) {
                        objectUpdates = new LinkedList<CRDTObjectUpdatesGroup<?>>();
                        objectsUpdates.put(u.getTargetUID(), objectUpdates);
                    }
                    objectUpdates.add(u.strippedWithCopiedTimestampMappings());
                    iter.remove();
                }
            }

            // Update client in any case.
            // if (objectsUpdates.isEmpty()) {
            // return null;
            // }
            // TODO: for clients that cannot receive periodical updates (e.g.
            // mobile in background mode), one could require a transaction to
            // declare his read set and force refresh the cache before the
            // transaction if necessary.

            if (FAKE_PRACTI_DEPOT_VECTORS) {
                final CausalityClock fakeVector = ClockFactory.newClock();
                // We compare against the stale vector matchin snapshot, but
                // these entries
                // would need to be eventually send anyways.
                for (final String clientId : dataServer.cltClock.getSiteIds()) {
                    final Timestamp clientTimestamp = dataServer.cltClock.getLatest(clientId);
                    if (!clientFakeVectorKnowledge.includes(clientTimestamp)) {
                        fakeVector.recordAllUntil(clientTimestamp);
                        clientFakeVectorKnowledge.recordAllUntil(clientTimestamp);
                    }
                }
                clientFakeVectorKnowledge.merge(fakeVector);
                generalExecutor.execute(new Runnable() {
                    public void run() {
                        onNotification(new SwiftNotification(new BatchUpdatesNotification(snapshot, disasterSafe,
                                objectsUpdates, fakeVector)));
                    }
                });
            } else {
                generalExecutor.execute(new Runnable() {
                    public void run() {
                        onNotification(new SwiftNotification(new BatchUpdatesNotification(snapshot, disasterSafe,
                                objectsUpdates)));
                    }
                });
            }
            lastNotification = now;
            return snapshot;
        }
    }

    synchronized public ClientSession getSession(ClientRequest clientRequest) {
        ClientSession session = sessions.get(clientRequest.getClientId());
        if (session == null) {
            sessions.put(clientRequest.getClientId(), session = new ClientSession(clientRequest.getClientId(),
                    clientRequest.isDisasterSafeSession()));
        }
        return session;
    }

    synchronized public ClientSession getSession(String clientId, boolean disasterSafe) {
        ClientSession session = sessions.get(clientId);
        if (session == null)
            sessions.put(clientId, session = new ClientSession(clientId, disasterSafe));
        return session;
    }

    private Map<String, ClientSession> sessions = new HashMap<String, ClientSession>();
    private static LWWStringMapRegisterUpdate FAKE_INIT_UPDATE = new LWWStringMapRegisterUpdate(1,
            AbstractLWWRegisterCRDT.INIT_TIMESTAMP, new HashMap<String, String>());

    // Map<String, Object> evaluateStaleReads(FetchObjectVersionRequest request,
    // CausalityClock estimatedDCVersionCopy,
    // CausalityClock estimatedDCStableVersionCopy) {
    //
    // Map<String, Object> res = new HashMap<String, Object>();
    //
    // ManagedCRDT mostRecentDCVersion = getCRDT(request.getUid(),
    // estimatedDCVersionCopy, request.getClientId());
    //
    // ManagedCRDT mostRecentScoutVersion = getCRDT(request.getUid(),
    // request.getClock(), request.getClientId());
    //
    // if (mostRecentDCVersion != null && mostRecentScoutVersion != null) {
    //
    // List<TimestampMapping> diff1 =
    // mostRecentScoutVersion.getUpdatesTimestampMappingsSince(request
    // .getDistasterDurableClock());
    //
    // List<TimestampMapping> diff2 =
    // mostRecentDCVersion.getUpdatesTimestampMappingsSince(request
    // .getDistasterDurableClock());
    //
    // List<TimestampMapping> diff3 = mostRecentDCVersion
    // .getUpdatesTimestampMappingsSince(estimatedDCStableVersionCopy);
    //
    // res.put("timestamp", request.timestamp);
    // res.put("Diff1-scout-normal-vs-scout-stable", diff1.size());
    // res.put("Diff2-dc-normal-vs-scout-stable", diff2.size());
    // res.put("Diff3-dc-normal-vs-dc-stable", diff3.size());
    // // res.put("dc-estimatedVersion", estimatedDCVersionCopy);
    // // res.put("dc-estimatedStableVersion",
    // // estimatedDCStableVersionCopy);
    // // res.put("scout-version", request.getVersion());
    // // res.put("scout-clock", request.getClock());
    // // res.put("scout-stableClock", request.getDistasterDurableClock());
    // // res.put("crdt", mostRecentDCVersion.clock);
    // }
    // return res;
    // }
}
