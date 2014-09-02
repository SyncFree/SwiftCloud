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
import static sys.net.api.Networking.TransportProvider.DEFAULT;
import static sys.net.api.Networking.TransportProvider.INPROC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.proto.BatchCommitUpdatesReply;
import swift.proto.BatchCommitUpdatesRequest;
import swift.proto.BatchFetchObjectVersionReply;
import swift.proto.BatchFetchObjectVersionReply.FetchStatus;
import swift.proto.BatchFetchObjectVersionRequest;
import swift.proto.ClientRequest;
import swift.proto.CommitTSReply;
import swift.proto.CommitTSRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesReply.CommitStatus;
import swift.proto.CommitUpdatesRequest;
import swift.proto.GenerateDCTimestampReply;
import swift.proto.GenerateDCTimestampRequest;
import swift.proto.LatestKnownClockReply;
import swift.proto.LatestKnownClockRequest;
import swift.proto.PingReply;
import swift.proto.PingRequest;
import swift.proto.SeqCommitUpdatesRequest;
import swift.proto.SwiftProtocolHandler;
import swift.pubsub.BatchUpdatesNotification;
import swift.pubsub.SurrogatePubSubService;
import swift.pubsub.SwiftSubscriber;
import swift.pubsub.UpdateNotification;
import swift.utils.FutureResultHandler;
import swift.utils.SafeLog;
import swift.utils.SafeLog.ReportType;
import sys.Sys;
import sys.dht.DHT_Node;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSubNotification;
import sys.pubsub.RemoteSubscriber;
import sys.pubsub.impl.AbstractSubscriber;
import sys.scheduler.PeriodicTask;
import sys.utils.Args;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
final public class DCSurrogate extends SwiftProtocolHandler {
    private final boolean notificationsSendFakePractiDepotVectors;
    private final boolean notificationsSendDeltaVectorsOnly;
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
    AtomicReference<CausalityClock> estimatedDCStableVersionShadow; // read
                                                                    // only
    // estimate
    // of
    // current
    // DC
    // state

    final public SurrogatePubSubService suPubSub;
    public int pubsubPort;

    final ThreadPoolExecutor crdtExecutor;
    final ThreadPoolExecutor generalExecutor;
    private final int notificationPeriodMillis;

    final ThreadLocal<Random> timeSmootherRandom;

    DCSurrogate(String siteId, int port4Clients, int port4Sequencers, Endpoint sequencerEndpoint, Properties props) {
        this.siteId = siteId;
        this.surrogateId = "s" + System.nanoTime();
        this.pubsubPort = port4Clients + 1;
        this.timeSmootherRandom = new ThreadLocal<Random>() {
            @Override
            protected Random initialValue() {
                return new Random();
            }
        };

        this.sequencerServerEndpoint = sequencerEndpoint;
        this.srvEndpoint4Clients = Networking.rpcBind(port4Clients).toDefaultService();

        TransportProvider provider = Args.contains("-integrated") ? INPROC : DEFAULT;
        this.cltEndpoint4Sequencer = Networking.rpcConnect(provider).toDefaultService();
        this.srvEndpoint4Sequencer = Networking.rpcBind(port4Sequencers, provider).toDefaultService();

        srvEndpoint4Clients.setHandler(this);
        srvEndpoint4Sequencer.setHandler(this);

        srvEndpoint4Clients.getFactory().setExecutor(Executors.newCachedThreadPool());
        srvEndpoint4Sequencer.getFactory().setExecutor(Executors.newFixedThreadPool(2));

        ArrayBlockingQueue<Runnable> crdtWorkQueue = new ArrayBlockingQueue<Runnable>(512);
        crdtExecutor = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, crdtWorkQueue);
        crdtExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        ArrayBlockingQueue<Runnable> generalWorkQueue = new ArrayBlockingQueue<Runnable>(512);
        generalExecutor = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, generalWorkQueue);
        generalExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        suPubSub = new SurrogatePubSubService(generalExecutor, this);
        dataServer = new DCDataServer(this, props, suPubSub, port4Clients + 2);

        final String notificationPeriodString = props.getProperty(DCConstants.NOTIFICATION_PERIOD_PROPERTY);
        if (notificationPeriodString != null) {
            notificationPeriodMillis = Integer.valueOf(notificationPeriodString);
        } else {
            notificationPeriodMillis = DCConstants.DEFAULT_NOTIFICATION_PERIOD_MS;
        }
        this.notificationsSendFakePractiDepotVectors = Boolean.valueOf(props.getProperty(
                DCConstants.NOTIFICATIONS_SEND_FAKE_PRACTI_DEPOT_VECTORS_PROPERTY,
                DCConstants.DEFAULT_NOTIFICATIONS_SEND_FAKE_PRACTI_DEPOT_VECTORS));
        this.notificationsSendDeltaVectorsOnly = Boolean.valueOf(props.getProperty(
                DCConstants.NOTIFICATIONS_SEND_DELTA_VECTORS_PROPERTY,
                DCConstants.DEFAULT_NOTIFICATIONS_SEND_DELTA_VECTORS));

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

    public void onReceive(RpcHandle conn, final BatchFetchObjectVersionRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("BatchFetchObjectVersionRequest client = " + request.getClientId() + "; crdt id = "
                    + request.getUids());
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

        if (request.hasSubscription()) {
            for (final CRDTIdentifier id : request.getUids()) {
                getSession(request).subscribe(id);
            }
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

        // TODO: for nodes !request.isDisasterSafe() send it less
        // frequently (it's for pruning only)
        final CausalityClock vvReply = !request.isDisasterSafeSession() && request.isSendDCVector() ? estimatedDCVersionCopy
                .clone() : null;

        final BatchFetchObjectVersionReply reply = new BatchFetchObjectVersionReply(request.getBatchSize(), vvReply,
                disasterSafeVVReply);

        final Semaphore sem = new Semaphore(0);
        for (int i = 0; i < request.getBatchSize(); i++) {
            final int finalIdx = i;
            dataServer.getCRDT(request.getUid(i), request.getKnownVersion(), request.getVersion(),
                    request.getClientId(), request.isSendMoreRecentUpdates(), request.hasSubscription(),
                    new FutureResultHandler<ManagedCRDT>() {
                        @Override
                        public void onResult(ManagedCRDT crdt) {
                            try {
                                adaptGetReplyToFetchReply(request, finalIdx, cltLastSeqNo, finalCmpClk,
                                        finalEstimatedDCVersionCopy, reply, crdt);
                            } finally {
                                sem.release();
                            }
                        }
                    });
        }
        sem.acquireUninterruptibly(request.getBatchSize());

        if (request.getBatchSize() > 1 && !request.isSendMoreRecentUpdates()) {
            final CausalityClock commonPruneClock = request.getVersion().clone();
            final CausalityClock commonClock = commonPruneClock.clone();
            if (cltLastSeqNo != null) {
                commonClock.recordAllUntil(cltLastSeqNo);
            }
            reply.compressAllOKReplies(commonPruneClock, commonClock);
        }

        conn.reply(reply);
    }

    private void adaptGetReplyToFetchReply(final BatchFetchObjectVersionRequest request, int idxInBatch,
            final Timestamp cltLastSeqNo, final CMP_CLOCK versionToDcCmpClock,
            final CausalityClock estimatedDCVersionClock, final BatchFetchObjectVersionReply reply, ManagedCRDT crdt) {
        if (crdt == null) {
            if (request.getKnownVersion() == null) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("BatchFetchObjectVersionRequest not found:" + request.getUid(idxInBatch));
                }
                reply.setReply(idxInBatch, BatchFetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null);
            } else {
                reply.setReply(idxInBatch, FetchStatus.UP_TO_DATE, null);
            }
        } else {
            synchronized (crdt) {
                crdt.augmentWithDCClockWithoutMappings(estimatedDCVersionClock);
                if (cltLastSeqNo != null)
                    crdt.augmentWithScoutClockWithoutMappings(cltLastSeqNo);

                // TODO: move it to data nodes
                if (!request.isSendMoreRecentUpdates()) {
                    CausalityClock restriction = (CausalityClock) request.getVersion().copy();
                    if (cltLastSeqNo != null) {
                        restriction.recordAllUntil(cltLastSeqNo);
                    }
                    crdt.discardRecentUpdates(restriction);
                }

                final BatchFetchObjectVersionReply.FetchStatus status;
                if (versionToDcCmpClock.is(CMP_CLOCK.CMP_ISDOMINATED, CMP_CLOCK.CMP_CONCURRENT)) {
                    logger.warning("Requested version " + request.getVersion() + " of object "
                            + request.getUid(idxInBatch) + " missing; local version: " + estimatedDCVersionClock
                            + " pruned as of " + crdt.getPruneClock());
                    status = FetchStatus.VERSION_MISSING;
                } else if (crdt.getPruneClock().compareTo(request.getVersion())
                        .is(CMP_CLOCK.CMP_DOMINATES, CMP_CLOCK.CMP_CONCURRENT)) {
                    logger.warning("Requested version " + request.getVersion() + " of object "
                            + request.getUid(idxInBatch) + " is pruned; local version: " + estimatedDCVersionClock
                            + " pruned as of " + crdt.getPruneClock());
                    status = FetchStatus.VERSION_PRUNED;
                } else {
                    status = FetchStatus.OK;
                }

                if (logger.isLoggable(Level.INFO)) {
                    logger.info("BatchFetchObjectVersionRequest clock = " + crdt.getClock() + "/"
                            + request.getUid(idxInBatch));
                }
                reply.setReply(idxInBatch, status, crdt);
            }
        }
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
            prepareAndDoCommit(session, request, new FutureResultHandler<CommitUpdatesReply>() {
                @Override
                public void onResult(CommitUpdatesReply result) {
                    conn.reply(result);
                }
            });
    }

    private void prepareAndDoCommit(final ClientSession session, final CommitUpdatesRequest req,
            final FutureResultHandler<CommitUpdatesReply> resHandler) {
        final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();
        final CausalityClock dependenciesClock = ops.size() > 0 ? req.getDependencyClock() : ClockFactory.newClock();

        GenerateDCTimestampReply tsReply = cltEndpoint4Sequencer.request(sequencerServerEndpoint,
                new GenerateDCTimestampRequest(req.getClientId(), req.isDisasterSafeSession(), req.getCltTimestamp(),
                        dependenciesClock));

        req.setTimestamp(tsReply.getTimestamp());

        // req.setDisasterSafe(); // FOR SOSP EVALUATION...

        doOneCommit(session, req, dependenciesClock, resHandler);
    }

    private void doOneCommit(final ClientSession session, final CommitUpdatesRequest req,
            final CausalityClock snapshotClock, final FutureResultHandler<CommitUpdatesReply> resHandler) {
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
        cltEndpoint4Sequencer.send(sequencerServerEndpoint, new CommitTSRequest(txTs, cltTs, prvCltTs,
                estimatedDCVersionCopy, txnOK.get(), ops, req.disasterSafe(), session.clientId),
                new SwiftProtocolHandler() {
                    public void onReceive(CommitTSReply reply) {
                        if (logger.isLoggable(Level.INFO)) {
                            logger.info("Commit: received CommitTSRequest:old vrs:" + estimatedDCVersionCopy
                                    + "; new vrs=" + reply.getCurrVersion() + ";ts = " + txTs + ";cltts = " + cltTs);
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
                            resHandler.onResult(new CommitUpdatesReply(txTs));
                            // return new CommitUpdatesReply(txTs);
                        } else {
                            // FIXME: CommitTSStatus.FAILED if not well
                            // documented. How comes it can fail?
                            logger.warning("Commit: failed for request " + req);
                            resHandler.onResult(new CommitUpdatesReply());
                        }
                    }
                }, 0);

        // if (reply == null)
        // logger.severe(String.format("------------>REPLY FROM SEQUENCER NULL for: %s, who:%s\n",
        // txTs,
        // session.clientId));
        //
        // } while (reply == null);
        //
        // if (reply != null) {
        //
        // }
        // return new CommitUpdatesReply();
    }

    @Override
    public void onReceive(final RpcHandle conn, final BatchCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("BatchCommitUpdatesRequest client = " + request.getClientId() + ":batch size="
                    + request.getCommitRequests().size());
        }
        final ClientSession session = getSession(request);
        if (logger.isLoggable(Level.INFO)) {
            logger.info("BatchCommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo());
        }

        final List<Timestamp> tsLst = new LinkedList<Timestamp>();
        final List<CommitUpdatesReply> reply = new LinkedList<CommitUpdatesReply>();

        for (CommitUpdatesRequest r : request.getCommitRequests()) {
            if (session.getLastSeqNo() != null
                    && session.getLastSeqNo().getCounter() >= r.getCltTimestamp().getCounter()) {
                reply.add(new CommitUpdatesReply(getEstimatedDCVersionCopy()));
                // FIXME: unless the timestamp is stable andpruned, we need to
                // send a precise mapping to the client!
                // Also, non-stable clock should appear in internal dependencies
                // in the batch.
            } else {
                // FIXME: is it required to respect internal dependencies in the
                // batch? Order in the local DC is respected already.
                // r.addTimestampsToDeps(tsLst);
                final Semaphore sem = new Semaphore(0);
                prepareAndDoCommit(session, r, new FutureResultHandler<CommitUpdatesReply>() {
                    @Override
                    public void onResult(CommitUpdatesReply repOne) {
                        if (repOne.getStatus() == CommitStatus.COMMITTED_WITH_KNOWN_TIMESTAMPS) {
                            List<Timestamp> tsLstOne = repOne.getCommitTimestamps();
                            if (tsLstOne != null)
                                tsLst.addAll(tsLstOne);
                        }
                        reply.add(repOne);
                        sem.release();
                    }
                });
                sem.acquireUninterruptibly();
            }
        }
        conn.reply(new BatchCommitUpdatesReply(reply));
    }

    @Override
    public void onReceive(final RpcHandle conn, final SeqCommitUpdatesRequest request) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("SeqCommitUpdatesRequest timestamp = " + request.getTimestamp() + ";clt="
                    + request.getCltTimestamp());
        }
        ClientSession session = getSession(request.getCltTimestamp().getIdentifier(), false);
        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
        CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
        doOneCommit(session, request, snapshotClock, new FutureResultHandler<CommitUpdatesReply>() {
            @Override
            public void onResult(CommitUpdatesReply result) {
                conn.reply(result);
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

    public class ClientSession extends AbstractSubscriber<CRDTIdentifier> implements SwiftSubscriber {

        final String clientId;
        boolean disasterSafe;
        volatile Timestamp lastSeqNo;
        private CausalityClock clientFakeVectorKnowledge;
        private CausalityClock lastSnapshotVector;

        private volatile RemoteSubscriber<CRDTIdentifier> remoteClient;
        private PeriodicTask notificationsTask;

        ClientSession(String clientId, boolean disasterSafe) {
            super(clientId);
            this.clientId = clientId;
            this.disasterSafe = disasterSafe;
            if (notificationsSendFakePractiDepotVectors && notificationsSendDeltaVectorsOnly) {
                clientFakeVectorKnowledge = ClockFactory.newClock();
            }
            if (notificationsSendDeltaVectorsOnly) {
                lastSnapshotVector = ClockFactory.newClock();
            }
        }

        // idempotent
        public synchronized void initNotifications() {
            if (notificationsTask != null) {
                return;
            }
            notificationsTask = new PeriodicTask(timeSmootherRandom.get().nextDouble() * notificationPeriodMillis
                    * 0.001, notificationPeriodMillis * 0.001) {
                public void run() {
                    tryFireClientNotification();
                    if (remoteClient != null && remoteClient.isOffline())
                        super.cancel();
                }
            };
        }

        public CausalityClock getMinVV() {
            return suPubSub.minDcVersion();
        }

        public ClientSession setClientEndpoint(Endpoint remote) {
            remoteClient = new RemoteSubscriber<CRDTIdentifier>(clientId, suPubSub.endpoint(), remote);
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

            List<CRDTObjectUpdatesGroup<?>> updates = update.info.getUpdates();
            if (updates.isEmpty() || clientId.equals(updates.get(0).getClientTimestamp().getIdentifier())) {
                // Ignore
                return;
            }

            pending.addAll(update.info.getUpdates());

            tryFireClientNotification();
        }

        protected synchronized CausalityClock tryFireClientNotification() {
            long now = Sys.Sys.timeMillis();
            if (now <= (lastNotification + notificationPeriodMillis)) {
                return null;
            }

            final CausalityClock snapshot = suPubSub.minDcVersion();
            if (disasterSafe) {
                snapshot.intersect(getEstimatedDCStableVersionCopy());
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

            // TODO: for clients that cannot receive periodical updates (e.g.
            // mobile in background mode), one could require a transaction to
            // declare his read set and force refresh the cache before the
            // transaction if necessary.
            
            if (notificationsSendDeltaVectorsOnly) {
                for (final String dcId : lastSnapshotVector.getSiteIds()) {
                    if (lastSnapshotVector.includes(snapshot.getLatest(dcId))) {
                        snapshot.drop(dcId);
                    }
                }
                lastSnapshotVector.merge(snapshot);
            }

            final BatchUpdatesNotification batch;
            if (notificationsSendFakePractiDepotVectors) {
                final CausalityClock fakeVector;
                synchronized (dataServer.cltClock) {
                    // "dataServer.cltClock" represents a more recent state than
                    // "snapshot" clock, but all of its entries would need to be
                    // send later anyways, so it is fair to use it for metadata
                    // comparison.  
                    fakeVector = dataServer.cltClock.clone();
                }
                if (notificationsSendDeltaVectorsOnly) {
                    for (final String clientId : clientFakeVectorKnowledge.getSiteIds()) {
                        final Timestamp clientTimestamp = fakeVector.getLatest(clientId);
                        if (clientFakeVectorKnowledge.includes(clientTimestamp)) {
                            fakeVector.drop(clientTimestamp.getIdentifier());
                        }
                    }
                }
                clientFakeVectorKnowledge.merge(fakeVector);
                batch = new BatchUpdatesNotification(snapshot, disasterSafe, objectsUpdates, fakeVector);
            } else {
                batch = new BatchUpdatesNotification(snapshot, disasterSafe, objectsUpdates);
            }

            if (remoteClient != null)
                remoteClient.onNotification(batch.clone(remoteClient.nextSeqN()));

            lastNotification = now;
            return snapshot;
        }

        @Override
        public void onNotification(BatchUpdatesNotification evt) {
            Thread.dumpStack();
        }

        @Override
        public void onNotification(PubSubNotification<CRDTIdentifier> evt) {
            Thread.dumpStack();
        }
    }

    public ClientSession getSession(ClientRequest clientRequest) {
        return getSession(clientRequest.getClientId(), clientRequest.isDisasterSafeSession());
    }

    public ClientSession getSession(String clientId, boolean disasterSafe) {
        ClientSession session = sessions.get(clientId), nsession;
        if (session == null) {
            session = sessions.putIfAbsent(clientId, nsession = new ClientSession(clientId, disasterSafe));
            if (session == null) {
                session = nsession;
                session.initNotifications();
            }
            if (ReportType.IDEMPOTENCE_GUARD_SIZE.isEnabled()) {
                SafeLog.report(ReportType.IDEMPOTENCE_GUARD_SIZE, siteId, sessions.size());
            }
        }
        return session;
    }

    private ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<String, ClientSession>();

    // private static LWWStringMapRegisterUpdate FAKE_INIT_UPDATE = new
    // LWWStringMapRegisterUpdate(1,
    // AbstractLWWRegisterCRDT.INIT_TIMESTAMP, new HashMap<String, String>());

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
