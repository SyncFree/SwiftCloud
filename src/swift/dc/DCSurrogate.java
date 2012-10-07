package swift.dc;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import swift.client.proto.CommitUpdatesReply;
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
import swift.client.proto.GenerateTimestampReplyHandler;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.KeepaliveReply;
import swift.client.proto.KeepaliveReplyHandler;
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
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.dc.proto.CommitTSReply;
import swift.dc.proto.CommitTSReplyHandler;
import swift.dc.proto.CommitTSRequest;
import swift.dc.proto.DHTSendNotification;
import swift.dc.proto.GenerateDCTimestampReply;
import swift.dc.proto.GenerateDCTimestampReplyHandler;
import swift.dc.proto.GenerateDCTimestampRequest;
import swift.dc.proto.SeqCommitUpdatesRequest;
import sys.RpcServices;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.pubsub.PubSub;
import sys.pubsub.impl.PubSubService;
import sys.utils.Threading;
/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
class DCSurrogate extends Handler implements swift.client.proto.SwiftServer, PubSub.Handler<CRDTIdentifier,DHTSendNotification> {

    String surrogateId;
    RpcEndpoint endpoint;
    Endpoint sequencerServerEndpoint;
    RpcEndpoint sequencerClientEndpoint;
    DCDataServer dataServer;
    CausalityClock estimatedDCVersion; // estimate of current DC state
    CausalityClock estimatedDCStableVersion; // estimate of current DC state

    PubSub<CRDTIdentifier,DHTSendNotification> PubSub;
    
    Map<String, ClientPubInfo> sessions; // map clientId -> ClientPubInfo
    Map<CRDTIdentifier, Map<String, ClientPubInfo>> cltsObserving; // map
                                                                   // crdtIdentifier
                                                                   // ->
                                                                   // Map<clientId,ClientPubInfo>
    
    DCSurrogate(RpcEndpoint e, RpcEndpoint seqClt, Endpoint seqSrv, Properties props) {
        this.surrogateId = "s" + System.nanoTime();
        this.endpoint = e;
        this.sequencerServerEndpoint = seqSrv;
        this.sequencerClientEndpoint = seqClt;
        this.PubSub = new PubSubService<CRDTIdentifier, DHTSendNotification>(e, RpcServices.PUBSUB.ordinal());
        initData(props);
        initDumping();       
        this.endpoint.setHandler(this);
        DCConstants.DCLogger.info("Server ready...");
    }

    private void initDumping() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                List<ClientPubInfo> lstSession = new ArrayList<ClientPubInfo>();
                for (;;) {
                    try {
                        lstSession.clear();
                        synchronized (sessions) {
                            lstSession.addAll(sessions.values());
                        }                        
                        CausalityClock clk = getEstimatedDCVersionCopy();
                        CausalityClock stableClk = getEstimatedDCStableVersionCopy();
                        
                        long nextTime = Long.MAX_VALUE;
                        for( ClientPubInfo i : lstSession ) 
                        	nextTime = Math.min(nextTime, i.dumpNotificationsIfTimeout(clk, stableClk));
                        
                        long waitTime = Math.min( 5000, nextTime - System.currentTimeMillis());
                
                        if (waitTime > 0)
                                Threading.sleep(waitTime);

                    } catch (Exception e) {
                        // do nothing
                    }

                }
            }
        });
        t.setDaemon(true);
        t.setPriority(Thread.currentThread().getPriority() - 1);
        t.start();
    }

    private void initData(Properties props) {
        sessions = new HashMap<String, ClientPubInfo>();
        cltsObserving = new HashMap<CRDTIdentifier, Map<String, ClientPubInfo>>();
        estimatedDCVersion = ClockFactory.newClock();
        estimatedDCStableVersion = ClockFactory.newClock();
        dataServer = new DCDataServer(this, props);
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

    /**
     * Add client to start observing changes on CRDT id
     * 
     * @param observing
     *            fi true, client wants to receive updated; otherwise,
     *            notifications
     */
    void addToObserving(CRDTIdentifier id, boolean observing, CausalityClock clk, ClientPubInfo session) {
        synchronized (cltsObserving) {
            Map<String, ClientPubInfo> clts = cltsObserving.get(id);
            if (clts == null) {
                clts = new TreeMap<String, ClientPubInfo>();
                cltsObserving.put(id, clts);
            }
            if (clts.size() == 0)
                PubSub.subscribe(id, this);
            clts.put(session.getClientId(), session);
            if (observing)
                session.setObserving(clk, id, true);
            else
                session.setNotificating(clk, id, true);
        }
    }

    /**
     * Removes client from observing changes on CRDT id
     */
    void remFromObserving(CRDTIdentifier id, ClientPubInfo session) {
        synchronized (cltsObserving) {
            Map<String, ClientPubInfo> clts = cltsObserving.get(id);
            if (clts == null) {
                PubSub.unsubscribe(id, this);
                return;
            }
            clts.remove(session.getClientId());
            if (clts.size() == 0) {
                PubSub.unsubscribe(id, this);
                cltsObserving.remove(id);
            }
        }
    }

    @Override
    public void notify(CRDTIdentifier id, DHTSendNotification notification) {
        DCConstants.DCLogger.info("Surrogate: Notify new updates for:" + notification.getInfo().getId());

        synchronized (estimatedDCVersion) {
            estimatedDCVersion.merge(notification.getEstimatedDCVersion());
        }
        synchronized (estimatedDCStableVersion) {
            estimatedDCStableVersion.merge(notification.getEstimatedDCStableVersion());
        }
        
        synchronized (cltsObserving) {
            Map<String, ClientPubInfo> map = cltsObserving.get(id);
            if (map == null)
                return;
            
            for( ClientPubInfo i : map.values() ) {
                CausalityClock vrs = getEstimatedDCVersionCopy();
//                vrs.recordAllUntil(i.getLastSeqNo());
                CausalityClock stable = getEstimatedDCStableVersionCopy();
//                stable.recordAllUntil(i.getLastSeqNo());
                ObjectSubscriptionInfo inf = notification.getInfo();
//                if( inf != null)
//                    inf = inf.clone(i.getLastSeqNo());
                
            	i.addNotifications( inf, vrs, stable);
            }
        }
    }

    ClientPubInfo getSession(String clientId) {
        synchronized (sessions) {
            ClientPubInfo session = sessions.get(clientId);
            if (session == null) {
                session = new ClientPubInfo(clientId);
                sessions.put(clientId, session);
            }
            return session;
        }
    }

    /*
     * void notifyNewUpdates( ObjectSubscriptionInfo notification) {
     * DCConstants.DCLogger.info( "Notify new updates for:" +
     * notification.getId()); }
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
            CausalityClock trxVersion, Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs) {
        return dataServer.execCRDT(grp, snapshotVersion, trxVersion, txTs, cltTs, prvCltTs); // call
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
        DCConstants.DCLogger.info("FetchObjectVersionRequest client = " + request.getClientId());
        conn.reply(handleFetchVersionRequest(request));
    }

    @Override
    public void onReceive(RpcHandle conn, FetchObjectDeltaRequest request) {
        DCConstants.DCLogger.info("FetchObjectDeltaRequest client = " + request.getClientId());
        conn.reply(handleFetchVersionRequest(request));
    }

    private FetchObjectVersionReply handleFetchVersionRequest(FetchObjectVersionRequest request) {
        DCConstants.DCLogger.info("FetchObjectVersionRequest client = " + request.getClientId() + "; crdt id = "
                + request.getUid());
        final ClientPubInfo session = getSession(request.getClientId());
        
        
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
        CausalityClock estimatedDCStableVersionCopy = estimatedDCStableVersion.clone();

        CRDTObject<?> crdt = getCRDT(request.getUid(), request.getSubscriptionType(), request.getVersion(), request.getClientId());
        if (crdt == null) {
            if( cltLastSeqNo != null)
                estimatedDCVersionCopy.recordAllUntil(cltLastSeqNo);
            return new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null,
                    estimatedDCVersionCopy, ClockFactory.newClock(), estimatedDCVersionCopy, estimatedDCStableVersionCopy);
        } else {
            if (request.getSubscriptionType() != SubscriptionType.NONE) {
                if (request.getSubscriptionType() == SubscriptionType.NOTIFICATION)
                    addToObserving(request.getUid(), false, crdt.crdt.getClock().clone(), session);
                else if (request.getSubscriptionType() == SubscriptionType.UPDATES)
                    addToObserving(request.getUid(), true, crdt.crdt.getClock().clone(), session);
            }
            synchronized (crdt) {
                crdt.clock.merge(estimatedDCVersionCopy);
                if( cltLastSeqNo != null)
                    crdt.clock.recordAllUntil(cltLastSeqNo);
                final FetchObjectVersionReply.FetchStatus status = (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND
                        : FetchStatus.OK;
                DCConstants.DCLogger.info("END FetchObjectVersionRequest clock = " + crdt.clock);
                return new FetchObjectVersionReply(status, crdt.crdt, crdt.clock, crdt.pruneClock,
                        estimatedDCVersionCopy, estimatedDCStableVersionCopy);
            }
        }
    }

    @Override
    public void onReceive(final RpcHandle conn, GenerateTimestampRequest request) {
        DCConstants.DCLogger.info("GenerateTimestampRequest client = " + request.getClientId());

        final ClientPubInfo session = getSession(request.getClientId());

        RpcHandle res = sequencerClientEndpoint.send(sequencerServerEndpoint, request, new GenerateTimestampReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn0, GenerateTimestampReply reply) {
                DCConstants.DCLogger.info("GenerateTimestampRequest: forwarding reply");
                conn.reply(reply);
            }
        }, 15);
        if( res.failed() )
        	System.out.println( request.getClass() + "-> failed...");
    }

    @Override
    public void onReceive(final RpcHandle conn, KeepaliveRequest request) {
        DCConstants.DCLogger.info("KeepaliveRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession(request.getClientId());

        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new KeepaliveReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn0, KeepaliveReply reply) {
                DCConstants.DCLogger.info("KeepaliveRequest: forwarding reply");
                conn.reply(reply);
            }
        });
    }
    
    protected void doProcessCommit( final ClientPubInfo session, final RpcHandle conn, final CommitUpdatesRequest request, final GenerateDCTimestampReply reply) {
//    0) updates.addSystemTimestamp(timestampService.allocateTimestamp())
//    1) let int clientTxs =
//    clientTxClockService.getAndLockNumberOfCommitedTxs(clientId)
//    2) for all modified objects:
//    crdt.augumentWithScoutClock(new Timestamp(clientId, clientTxs)) //
//    ensures that execute() has enough information to ensure tx idempotence
//    crdt.execute(updates...)
//    crdt.discardScoutClock(clientId) // critical to not polute all data
//    nodes and objects with big vectors, unless we want to do it until
//    pruning
//    3) clientTxClockService.unlock(clientId)
    
        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
        final Timestamp txTs = reply.getTimestamp();
        final Timestamp cltTs = request.getClientTimestamp();
        final Timestamp prvCltTs = session.getLastSeqNo();
        for( CRDTObjectUpdatesGroup<?> o: ops) {
            o.addSystemTimestamp(txTs);
        }
        
        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
        final CausalityClock trxClock = snapshotClock.clone();
        trxClock.record(txTs);
        Iterator<CRDTObjectUpdatesGroup<?>> it = ops.iterator();
        final ExecCRDTResult[] results = new ExecCRDTResult[ops.size()]; 
        boolean ok = true;
        int pos = 0;
        while (it.hasNext()) {
            // TODO: must make this concurrent to be fast
            CRDTObjectUpdatesGroup<?> grp = it.next();
            results[pos] = execCRDT(grp, snapshotClock, trxClock, txTs, cltTs, prvCltTs);
            ok = ok && results[pos].isResult();
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(grp.getDependency());
            }
            pos++;
        }
        final boolean txResult = ok;
        // TODO: handle failure

        CausalityClock estimatedDCVersionCopy0 = null;
        synchronized (estimatedDCVersion) {
            estimatedDCVersionCopy0 = estimatedDCVersion.clone();
        }
        final CausalityClock estimatedDCVersionCopy = estimatedDCVersionCopy0;
        session.setLastSeqNo( cltTs);
        sequencerClientEndpoint.send(sequencerServerEndpoint, new CommitTSRequest(txTs, cltTs, prvCltTs, 
                estimatedDCVersionCopy, ok,
                request.getObjectUpdateGroups()), new CommitTSReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn0, CommitTSReply reply) {
                DCConstants.DCLogger.info("Commit: received CommitTSRequest:old vrs:" + estimatedDCVersionCopy + 
                        "; new vrs=" + reply.getCurrVersion() + ";ts = " + txTs + ";cltts = " + cltTs);
                CausalityClock estimatedDCVersionCopy = null;
                synchronized (estimatedDCVersion) {
                    estimatedDCVersion.merge(reply.getCurrVersion());
                    estimatedDCVersionCopy = estimatedDCVersion.clone();
                }
                CausalityClock estimatedDCStableVersionCopy = null;
                synchronized (estimatedDCStableVersion) {
                    estimatedDCStableVersion.merge(reply.getStableVersion());
                    estimatedDCStableVersionCopy = estimatedDCStableVersion.clone();
                }
                if (txResult && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                    conn.reply(new CommitUpdatesReply(txTs));
                    for( int i = 0; i < results.length; i++) {
                        ExecCRDTResult result = results[i];
                        if( result == null)
                            continue;
                        if( result.hasNotification()) {
                            if( results[i].isNotificationOnly()) {
                                Thread.dumpStack();
                                PubSub.publish(result.getId(), new DHTSendNotification(result.getInfo().cloneNotification(), estimatedDCVersionCopy, estimatedDCStableVersionCopy));
                            } else {
                                PubSub.publish(result.getId(), new DHTSendNotification(result.getInfo(), estimatedDCVersionCopy, estimatedDCStableVersionCopy));
                            }
                            
                        }
                    }
                } else {
                    conn.reply(new CommitUpdatesReply());
                }
            }
        });
        
    }

    @Override
    public void onReceive(final RpcHandle conn, final CommitUpdatesRequest request) {
        DCConstants.DCLogger.info("CommitUpdatesRequest client = " + request.getClientId() + 
                ":ts=" + request.getClientTimestamp()+
                ":nops=" + request.getObjectUpdateGroups().size());
        final ClientPubInfo session = getSession(request.getClientId());
        DCConstants.DCLogger.info("CommitUpdatesRequest ... lastSeqNo=" + session.getLastSeqNo()); 
        
        if( session.getLastSeqNo() != null && session.getLastSeqNo().getCounter() >= request.getClientTimestamp().getCounter()) {
            conn.reply(new CommitUpdatesReply( getEstimatedDCVersionCopy()));
            return;
        }
        
        sequencerClientEndpoint.send(sequencerServerEndpoint, new GenerateDCTimestampRequest( request.getClientId(), 
                    request.getClientTimestamp(), request.getObjectUpdateGroups().size() > 0 ? request.getObjectUpdateGroups().get(0).getDependency() : ClockFactory.newClock()), 
                new GenerateDCTimestampReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn0, GenerateDCTimestampReply reply) {
                doProcessCommit( session, conn, request, reply);
            }
        });
    }

//    @Override
//    public void onReceive(final RpcHandle conn, CommitUpdatesRequest request) {
//        DCConstants.DCLogger.info("CommitUpdatesRequest client = " + request.getClientId() + ":ts=" + request.getBaseTimestamp()+":nops=" + request.getObjectUpdateGroups().size());
//        final ClientPubInfo session = getSession(request.getClientId());
//
//        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
//        final Timestamp ts = request.getBaseTimestamp();
//        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
//        final CausalityClock trxClock = snapshotClock.clone();
//        trxClock.record(request.getBaseTimestamp());
//        Iterator<CRDTObjectUpdatesGroup<?>> it = ops.iterator();
//        final ExecCRDTResult[] results = new ExecCRDTResult[ops.size()]; 
//        boolean ok = true;
//        int pos = 0;
//        while (it.hasNext()) {
//            // TODO: must make this concurrent to be fast
//            CRDTObjectUpdatesGroup<?> grp = it.next();
//            results[pos] = execCRDT(grp, snapshotClock, trxClock);
//            ok = ok && results[pos].isResult();
//            synchronized (estimatedDCVersion) {
//                estimatedDCVersion.merge(grp.getDependency());
//            }
//            pos++;
//        }
//        final boolean txResult = ok;
//        // TODO: handle failure
//
//        CausalityClock estimatedDCVersionCopy = null;
//        synchronized (estimatedDCVersion) {
//            estimatedDCVersionCopy = estimatedDCVersion.clone();
//        }
//        sequencerClientEndpoint.send(sequencerServerEndpoint, new CommitTSRequest(ts, estimatedDCVersionCopy, ok,
//                request.getObjectUpdateGroups(), request.getBaseTimestamp()), new CommitTSReplyHandler() {
//            @Override
//            public void onReceive(RpcHandle conn0, CommitTSReply reply) {
//                DCConstants.DCLogger.info("Commit: received CommitTSRequest");
//                CausalityClock estimatedDCVersionCopy = null;
//                synchronized (estimatedDCVersion) {
//                    estimatedDCVersion.merge(reply.getCurrVersion());
//                    estimatedDCVersionCopy = estimatedDCVersion.clone();
//                }
//                if (txResult && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
//                    conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.COMMITTED, ts));
//                    for( int i = 0; i < results.length; i++) {
//                        ExecCRDTResult result = results[i];
//                        if( result == null)
//                            continue;
//                        if( result.hasNotification()) {
//                            if( results[i].isNotificationOnly()) {
//                                PubSub.PubSub.publish( result.getId().toString(), new DHTSendNotification(result.getInfo().cloneNotification(), estimatedDCVersionCopy));
//                            } else {
//                                PubSub.PubSub.publish(result.getId().toString(), new DHTSendNotification(result.getInfo(), estimatedDCVersionCopy));
//                            }
//                            
//                        }
//                    }
//                } else {
//                    conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.INVALID_OPERATION, ts));
//                }
//            }
//        });
//    }

    private void updateEstimatedDCVersion() {
        sequencerClientEndpoint.send(sequencerServerEndpoint, new LatestKnownClockRequest("suurogate"),
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
        DCConstants.DCLogger.info("LatestKnownClockRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession(request.getClientId());
        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new LatestKnownClockReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn0, LatestKnownClockReply reply) {
                DCConstants.DCLogger.info("LatestKnownClockRequest: forwarding reply:" + reply.getClock());
                conn.reply(reply);
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
    public void onReceive(RpcHandle conn, UnsubscribeUpdatesRequest request) {
        DCConstants.DCLogger.info("UnsubscribeUpdatesRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession(request.getClientId());
        remFromObserving(request.getUid(), session);
    }

    @Override
    public void onReceive(RpcHandle conn, RecentUpdatesRequest request) {
        DCConstants.DCLogger.info("RecentUpdatesRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession(request.getClientId());
        session.dumpNewUpdates(conn, request);
    }

    @Override
    public void onReceive(RpcHandle conn, FastRecentUpdatesRequest request) {
        DCConstants.DCLogger.info("FastRecentUpdatesRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession(request.getClientId());
        session.dumpNewUpdates(conn, request, getEstimatedDCVersionCopy(), getEstimatedDCStableVersionCopy());
    }

    @Override
    public void onReceive(RpcHandle conn, SeqCommitUpdatesRequest request) {
        DCConstants.DCLogger.info("SeqCommitUpdatesRequest timestamp = " + request.getTimestamp() + ";clt=" + request.getCltTimestamp());

        List<CRDTObjectUpdatesGroup<?>> ops = request.getObjectUpdateGroups();
        final Timestamp ts = request.getTimestamp();
        final Timestamp cltTs = request.getCltTimestamp();
        final Timestamp prvCltTs = request.getPrvCltTimestamp();
        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency() : ClockFactory.newClock();
        final CausalityClock trxClock = snapshotClock.clone();
        trxClock.record(request.getTimestamp());
        Iterator<CRDTObjectUpdatesGroup<?>> it = ops.iterator();
        final ExecCRDTResult[] results = new ExecCRDTResult[ops.size()]; 
        boolean ok = true;
        int pos = 0;
        while (it.hasNext()) {
            // TODO: must make this concurrent to be fast
            CRDTObjectUpdatesGroup<?> grp = it.next();
            results[pos] = execCRDT(grp, snapshotClock, trxClock, request.getTimestamp(), 
                    request.getCltTimestamp(), request.getPrvCltTimestamp());
            ok = ok && results[pos].isResult();
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(grp.getDependency());
            }
            pos++;
        }
        final boolean txResult = ok;
        // TODO: handle failure

        CausalityClock estimatedDCVersionCopy = null;
        synchronized (estimatedDCVersion) {
            estimatedDCVersionCopy = estimatedDCVersion.clone();
        }
        sequencerClientEndpoint.send(sequencerServerEndpoint, new CommitTSRequest(ts, cltTs, prvCltTs, estimatedDCVersionCopy, ok,
                request.getObjectUpdateGroups()), new CommitTSReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn0, CommitTSReply reply) {
                DCConstants.DCLogger.info("Commit: received CommitTSRequest");
                CausalityClock estimatedDCVersionCopy = null;
                synchronized (estimatedDCVersion) {
                    estimatedDCVersion.merge(reply.getCurrVersion());
                    estimatedDCVersionCopy = estimatedDCVersion.clone();
                }
                CausalityClock estimatedDCStableVersionCopy = null;
                synchronized (estimatedDCStableVersion) {
                    estimatedDCStableVersion.merge(reply.getStableVersion());
                    estimatedDCStableVersionCopy = estimatedDCStableVersion.clone();
                }
                if (txResult && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                    for( int i = 0; i < results.length; i++) {
                        ExecCRDTResult result = results[i];
                        if( result == null)
                            continue;
                        if( result.hasNotification()) {
                            if( results[i].isNotificationOnly()) {
                                PubSub.publish(result.getId(), new DHTSendNotification(result.getInfo().cloneNotification(), estimatedDCVersionCopy, estimatedDCStableVersionCopy));
                            } else {
                                PubSub.publish(result.getId(), new DHTSendNotification(result.getInfo(), estimatedDCVersionCopy, estimatedDCStableVersionCopy));
                            }
                            
                        }
                    }
                }
            }
        }, 0);
    }

}

class ClientPubInfo {
    private String clientId;
    boolean hasUpdates;
    RpcHandle conn;
    long replyTime;
    Timestamp lastSeqNo; // last sequence number seen from client
    
    private Map<CRDTIdentifier, CRDTSessionInfo> subscriptions;

    ClientPubInfo() {
    }

    public ClientPubInfo(String clientId) {
        this.clientId = clientId;
        hasUpdates = false;
        conn = null;
        replyTime = Long.MAX_VALUE;
        lastSeqNo = null;
        subscriptions = new TreeMap<CRDTIdentifier, CRDTSessionInfo>();
    }

    public synchronized boolean isObserving(CRDTIdentifier id) {
        CRDTSessionInfo info = subscriptions.get(id);
        if (info == null)
            return false;
        else
            return info.isObserving();
    }

    public synchronized boolean isNotificating(CRDTIdentifier id) {
        CRDTSessionInfo info = subscriptions.get(id);
        if (info == null)
            return false;
        else
            return info.isNotificating();
    }

    public synchronized boolean isNotificatingOrObserving(CRDTIdentifier id) {
        CRDTSessionInfo info = subscriptions.get(id);
        if (info == null)
            return false;
        else
            return info.isObserving() || info.isNotificating();
    }

    public synchronized void setObserving(CausalityClock clk, CRDTIdentifier id, boolean set) {
        CRDTSessionInfo info = subscriptions.get(id);
        if (info == null) {
            if (!set)
                return;
            info = new CRDTSessionInfo(clk, true, false);
            subscriptions.put(id, info);
        }
        info.setObserving(set);
        if (!info.isObserving() && !info.isNotificating()) {
            subscriptions.remove(id);
        }
    }

    public synchronized void setNotificating(CausalityClock clk, CRDTIdentifier id, boolean set) {
        CRDTSessionInfo info = subscriptions.get(id);
        if (info == null) {
            if (!set)
                return;
            info = new CRDTSessionInfo(clk, false, true);
            subscriptions.put(id, info);
        }
        info.setNotificating(set);
        if (!info.isObserving() && !info.isNotificating()) {
            subscriptions.remove(id);
        }
    }

    synchronized void dumpNewUpdates(RpcHandle conn, FastRecentUpdatesRequest request, CausalityClock clk, CausalityClock stableClk) {
        this.conn = conn;
//        this.conn.enableStreamingReplies(true); //smd
        
        replyTime = Math.min(replyTime, System.currentTimeMillis() + request.getMaxBlockingTimeMillis());
        if (hasUpdates) //smd - what's the protocol? client is using exponential backoff waiting for reply...but server does not send them always...
            dumpNotifications(clk, stableClk);
    }

    synchronized void dumpNewUpdates(RpcHandle conn, RecentUpdatesRequest request) {
        // TODO: return updates
    }

    public synchronized void addNotifications(ObjectSubscriptionInfo info, CausalityClock clk, CausalityClock stableClk) {
        CRDTSessionInfo session = subscriptions.get(info.getId());
        if (session == null)
            return;
        session.addNotifications(info);
        hasUpdates = true;
        dumpNotifications(clk, stableClk);
    }

    synchronized long dumpNotificationsIfTimeout(CausalityClock clk, CausalityClock stableClk) {
        if (conn != null && System.currentTimeMillis() > replyTime) {
            DCConstants.DCLogger.info("dumpNotificationsIfTimeout clientId = " + clientId + " =========================================");
            dumpNotifications(clk, stableClk);
        }
        if (conn == null)
            return Long.MAX_VALUE;
        else
            return replyTime;
    }

    private synchronized void dumpNotifications(CausalityClock clk, CausalityClock stableClk) {
        if (conn == null)
            return;
        List<ObjectSubscriptionInfo> notifications = new ArrayList<ObjectSubscriptionInfo>();
        SubscriptionStatus status = subscriptions.size() == 0 ? SubscriptionStatus.LOST : SubscriptionStatus.ACTIVE;
        Iterator<Entry<CRDTIdentifier, CRDTSessionInfo>> it = subscriptions.entrySet().iterator();
        while (it.hasNext()) {
            Entry<CRDTIdentifier, CRDTSessionInfo> entry = it.next();
            entry.getValue().addSubscriptionInfo(entry.getKey(), notifications);
        }
        conn.reply(new FastRecentUpdatesReply(status, notifications, clk, stableClk));

        hasUpdates = false;
        conn = null;
        replyTime = Long.MAX_VALUE;
    }

    public String getClientId() {
        return clientId;
    }

    public Timestamp getLastSeqNo() {
        return lastSeqNo;
    }

    public synchronized void setLastSeqNo(Timestamp lastSeqNo) {
        if( this.lastSeqNo == null || this.lastSeqNo.getCounter() < lastSeqNo.getCounter())
            this.lastSeqNo = lastSeqNo;
        this.notifyAll();
    }
    
    public synchronized void waitForLastSeqNo( long seqNo) {
        while( lastSeqNo.getCounter() < seqNo)
            try {
                wait();
            } catch (InterruptedException e) {
                // do nothing
            }
        
    }
}

/*
 * class ClientSession { String clientId; Map<CRDTIdentifier,CRDTSessionInfo>
 * subscriptions; RpcHandle conn;
 * 
 * ClientSession( String clientId) { this.clientId = clientId; subscriptions =
 * new TreeMap<CRDTIdentifier,CRDTSessionInfo>(); updates = new
 * TreeSet<CRDTIdentifier>(); notifications = new TreeSet<CRDTIdentifier>();
 * conn = null; }
 * 
 * void subscribeUpdates( CRDTIdentifier id) { updates.add(id); }
 * 
 * void unsubscribeUpdates( CRDTIdentifier id) { updates.remove(id); }
 * 
 * void subscribeNotifications( CRDTIdentifier id) { notifications.add(id); }
 * 
 * void unsubscribeNotifications( CRDTIdentifier id) { notifications.remove(id);
 * }
 * 
 * void addNotifications(ObjectSubscriptionInfo info) {
 * 
 * }
 * 
 * void dumpNewUpdates( RpcHandle conn, FastRecentUpdatesRequest request) {
 * //TODO: return notifications }
 * 
 * void dumpNewUpdates( RpcHandle conn, RecentUpdatesRequest request) {
 * //TODO: return updates }
 * 
 * public boolean equals( Object obj) { return obj instanceof ClientSession &&
 * ((ClientSession)obj).clientId.equals(clientId); }
 * 
 * public int hashCode() { return clientId.hashCode(); } }
 */

class CRDTSessionInfo {
    private boolean observing;
    private boolean notificating;
    private boolean hasChanges;
    private CausalityClock oldClock;
    private CausalityClock newClock;
    private List<CRDTObjectUpdatesGroup<?>> updates;

    public CRDTSessionInfo(CausalityClock clk, boolean observing, boolean notificating) {
        this.oldClock = clk;
        this.newClock = clk.clone();
        this.observing = observing;
        this.notificating = notificating;
        this.hasChanges = false;
        updates = new ArrayList<CRDTObjectUpdatesGroup<?>>();
    }

    public void addSubscriptionInfo(CRDTIdentifier id, List<ObjectSubscriptionInfo> notifications) {
        if (!hasChanges) {
            notifications.add(new ObjectSubscriptionInfo(id, oldClock, newClock, updates, false));
        } else {
            notifications.add(new ObjectSubscriptionInfo(id, oldClock, newClock, updates, true));
        }
        hasChanges = false;
        newClock = oldClock;
        updates.clear();
    }

    public void addNotifications(ObjectSubscriptionInfo info) {
        if (!hasChanges) {
            oldClock = info.getOldClock();
            newClock = info.getNewClock();
            updates.addAll(info.getUpdates());
            hasChanges = true;
        } else {
            // TODO: check if new clock == old clock
            newClock.merge(info.getNewClock());
            updates.addAll(info.getUpdates());
        }
    }

    public boolean isObserving() {
        return observing;
    }

    public void setObserving(boolean observing) {
        this.observing = observing;
    }

    public boolean isNotificating() {
        return notificating;
    }

    public void setNotificating(boolean notificating) {
        this.notificating = notificating;
    }

    public boolean isHasChanges() {
        return hasChanges;
    }

}

class Reply implements RpcMessage {

    Reply() {
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        if (conn.expectingReply()) {
            ((Handler) handler).onReceive(conn, this);
        } else {
            ((Handler) handler).onReceive(this);
        }
    }
}
