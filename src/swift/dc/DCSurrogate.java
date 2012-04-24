package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.client.proto.FastRecentUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReply;
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
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.dc.proto.CommitTSReply;
import swift.dc.proto.CommitTSReplyHandler;
import swift.dc.proto.CommitTSRequest;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
class DCSurrogate extends Handler implements swift.client.proto.SwiftServer {

    String surrogateId;
    RpcEndpoint endpoint;
    Endpoint sequencerServerEndpoint;
    RpcEndpoint sequencerClientEndpoint;
    DCDataServer dataServer;
    CausalityClock estimatedDCVersion;       // estimate of current DC state
    
    Map<String,ClientSession> sessions;   // map clientId -> clientSession
    Map<CRDTIdentifier,Set<ClientSession>> cltsObserving;   // map crdtIdentifier -> Set<ClientSession>
    Map<CRDTIdentifier,Set<ClientSession>> cltsNotificating;   // map crdtIdentifier -> Set<ClientSession>

    DCSurrogate(RpcEndpoint e, RpcEndpoint seqClt, Endpoint seqSrv, Properties props) {
        initData( props);
        this.surrogateId = "s" + System.nanoTime();
        this.endpoint = e;
        this.sequencerServerEndpoint = seqSrv;
        this.sequencerClientEndpoint = seqClt;
        this.endpoint.setHandler(this);
        DCConstants.DCLogger.info("Server ready...");
    }

    private void initData( Properties props) {
        sessions = new HashMap<String,ClientSession>();
        cltsObserving = new HashMap<CRDTIdentifier,Set<ClientSession>>();
        cltsNotificating = new HashMap<CRDTIdentifier,Set<ClientSession>>();
        dataServer = new DCDataServer( this, props);
        estimatedDCVersion = ClockFactory.newClock();
    }
    
    String getId() {
        return surrogateId;
    }
    
    void addToObserving(CRDTIdentifier id, ClientSession session) {
        session.subscribeUpdates(id);
        synchronized (cltsObserving) {
            Set<ClientSession> clts = cltsObserving.get(id);
            if (clts == null) {
                clts = new TreeSet<ClientSession>();
                cltsObserving.put(id, clts);
            }
            clts.add(session);
        }
    }

    void remFromObserving(CRDTIdentifier id, ClientSession clt) {
        clt.unsubscribeUpdates(id);
        synchronized (cltsObserving) {
            Set<ClientSession> clts = cltsObserving.get(id);
            if (clts != null) {
                cltsObserving.remove(clt);
                if(clts.size() == 0) {
                    cltsObserving.remove(id);
                }
            }
        }
    }

    void addToNotificating(CRDTIdentifier id, ClientSession session) {
        session.subscribeNotifications(id);
        synchronized (cltsNotificating) {
            Set<ClientSession> clts = cltsNotificating.get(id);
            if (clts == null) {
                clts = new TreeSet<ClientSession>();
                cltsNotificating.put(id, clts);
            }
            clts.add(session);
        }
    }

    void remFromNotificating(CRDTIdentifier id, ClientSession clt) {
        clt.unsubscribeNotifications(id);
        synchronized (cltsNotificating) {
            Set<ClientSession> clts = cltsNotificating.get(id);
            if (clts != null) {
                cltsNotificating.remove(clt);
                if(clts.size() == 0) {
                    cltsObserving.remove(id);
                }
            }
        }
    }
    ClientSession getSession(String clientId) {
        synchronized (sessions) {
            ClientSession session = sessions.get(clientId);
            if (session == null) {
                session = new ClientSession( clientId);
                sessions.put(clientId, session);
            }
            return session;
        }
    }
    
    void notifyNewUpdates( ObjectSubscriptionInfo notification) {
        DCConstants.DCLogger.info( "Notify new updates for:" + notification.getId());
    }
    
//    /**
//     * Return null if CRDT does not exist
//     */
//    <V extends CRDT<V>> CRDTData<V> putCRDT(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clk, CausalityClock prune) {
//        return dataServer.putCRDT(id, crdt, clk, prune);        // call DHT server
//    }

    
    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> boolean execCRDT(CRDTObjectOperationsGroup<V> grp, CausalityClock snapshotVersion, CausalityClock trxVersion) {
        return dataServer.execCRDT(grp, snapshotVersion, trxVersion);       // call DHT server
    }

    /**
     * Return null if CRDT does not exist
     * 
     * @param subscribe Subscription type
     */
    CRDTObject<?> getCRDT(CRDTIdentifier id, SubscriptionType subscribe) {
        CausalityClock clk = null;
        synchronized( estimatedDCVersion) {
            clk = estimatedDCVersion.clone();
        }
        CRDTObject<?> o = dataServer.getCRDT(id, subscribe);    // call DHT server
        if( o == null)
            return null;
        o.clock.merge(estimatedDCVersion);
        return o;
    }

    @Override
    public void onReceive(RpcConnection conn, FetchObjectVersionRequest request) {
        DCConstants.DCLogger.info("FetchObjectVersionRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());

        CMP_CLOCK cmp = CMP_CLOCK.CMP_EQUALS;
        synchronized( estimatedDCVersion) {
            cmp = request.getVersion() == null ? CMP_CLOCK.CMP_EQUALS : estimatedDCVersion.compareTo(request.getVersion());
        }
        if( cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) {
            updateEstimatedDCVersion();
            cmp = estimatedDCVersion.compareTo(request.getVersion());
        }
        if( cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) {
             conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.VERSION_NOT_FOUND, null, request
                    .getVersion(), ClockFactory.newClock()));
        } else {

        CRDTObject<?> crdt = getCRDT(request.getUid(), request.getSubscriptionType());
        if (crdt == null) {
            conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null, request
                    .getVersion(), ClockFactory.newClock()));
        } else {
            if( request.getSubscriptionType() != SubscriptionType.NONE) {
                if( request.getSubscriptionType() == SubscriptionType.NOTIFICATION)
                    addToNotificating(request.getUid(), session);
                else if( request.getSubscriptionType() == SubscriptionType.NOTIFICATION)
                    addToObserving(request.getUid(), session);
            }
            synchronized (crdt) {
                conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OK, crdt.crdt, crdt.clock,
                        crdt.pruneClock));
            }
        }
        }
    }

    @Override
    public void onReceive(RpcConnection conn, FetchObjectDeltaRequest request) {
        DCConstants.DCLogger.info("FetchObjectDeltaRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());

        CMP_CLOCK cmp = CMP_CLOCK.CMP_EQUALS;
        synchronized( estimatedDCVersion) {
            cmp = request.getVersion() == null ? CMP_CLOCK.CMP_EQUALS : estimatedDCVersion.compareTo(request.getVersion());
        }
        if( cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) {
            updateEstimatedDCVersion();
            cmp = estimatedDCVersion.compareTo(request.getVersion());
        }
        if( cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) {
             conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.VERSION_NOT_FOUND, null, request
                    .getVersion(), ClockFactory.newClock()));
        } else {

        CRDTObject<?> crdt = getCRDT(request.getUid(), request.getSubscriptionType());
        if (crdt == null) {
            conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null, request
                    .getVersion(), ClockFactory.newClock()));
        } else {
            if( request.getSubscriptionType() != SubscriptionType.NONE) {
                if( request.getSubscriptionType() == SubscriptionType.NOTIFICATION)
                    addToNotificating(request.getUid(), session);
                else if( request.getSubscriptionType() == SubscriptionType.NOTIFICATION)
                    addToObserving(request.getUid(), session);
            }
            synchronized (crdt) {
                conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OK, crdt.crdt, crdt.clock,
                        crdt.pruneClock));
            }
        }
        }

    }

    @Override
    public void onReceive(final RpcConnection conn, GenerateTimestampRequest request) {
        DCConstants.DCLogger.info("GenerateTimestampRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());

        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new GenerateTimestampReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, GenerateTimestampReply reply) {
                DCConstants.DCLogger.info("GenerateTimestampRequest: forwarding reply");
                conn.reply(reply);
            }
        });
    }

    @Override
    public void onReceive(final RpcConnection conn, KeepaliveRequest request) {
        DCConstants.DCLogger.info("KeepaliveRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());

        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new KeepaliveReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, KeepaliveReply reply) {
                DCConstants.DCLogger.info("KeepaliveRequest: forwarding reply");
                conn.reply(reply);
            }
        });
    }

    @Override
    public void onReceive(final RpcConnection conn, CommitUpdatesRequest request) {
        DCConstants.DCLogger.info("CommitUpdatesRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());

        List<CRDTObjectOperationsGroup<?>> ops = request.getObjectUpdateGroups();
        final Timestamp ts = request.getBaseTimestamp();
        final CausalityClock snapshotClock = ops.size() > 0 ? ops.get(0).getDependency(): ClockFactory.newClock();
        final CausalityClock trxClock = snapshotClock.clone();
        trxClock.record(request.getBaseTimestamp());
        Iterator<CRDTObjectOperationsGroup<?>> it = ops.iterator();
        boolean ok = true;
        while (it.hasNext()) {
            //TODO: must make this concurrent to be fast
            CRDTObjectOperationsGroup<?> grp = it.next();
            ok = ok && execCRDT(grp, snapshotClock, trxClock);
            synchronized (estimatedDCVersion) {
                estimatedDCVersion.merge(grp.getDependency());
            }
        }
        final boolean txResult = ok;
        // TODO: handle failure

        CausalityClock estimatedDCVersionCopy = null;
        synchronized (estimatedDCVersion) {
            estimatedDCVersionCopy = estimatedDCVersion.clone();
        }
        sequencerClientEndpoint.send(sequencerServerEndpoint, new CommitTSRequest(ts, estimatedDCVersionCopy, ok),
                new CommitTSReplyHandler() {
                    @Override
                    public void onReceive(RpcConnection conn0, CommitTSReply reply) {
                        DCConstants.DCLogger.info("Commit: received CommitTSRequest");
                        if (txResult && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                            synchronized (estimatedDCVersion) {
                                estimatedDCVersion.merge(reply.getCurrVersion());
                            }
                            conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.COMMITTED, ts));
                        } else {
                            conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.INVALID_OPERATION, ts));
                        }
                    }
                });
    }

    
    private void updateEstimatedDCVersion() {
        sequencerClientEndpoint.send(sequencerServerEndpoint, new LatestKnownClockRequest("suurogate"), new LatestKnownClockReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, LatestKnownClockReply reply) {
                synchronized( estimatedDCVersion) {
                    estimatedDCVersion.merge( reply.getClock());
                }
            }
        });
    }
    
    @Override
    public void onReceive(final RpcConnection conn, LatestKnownClockRequest request) {
        DCConstants.DCLogger.info("LatestKnownClockRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());
        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new LatestKnownClockReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, LatestKnownClockReply reply) {
                DCConstants.DCLogger.info("LatestKnownClockRequest: forwarding reply:" + reply.getClock());
                conn.reply(reply);
                synchronized( estimatedDCVersion) {
                    estimatedDCVersion.merge( reply.getClock());
                }
            }
        });
    }

    @Override
    public void onReceive(RpcConnection conn, UnsubscribeUpdatesRequest request) {
        DCConstants.DCLogger.info("UnsubscribeUpdatesRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());
        //TODO: handle unssubscribe
    }

    @Override
    public void onReceive(RpcConnection conn, RecentUpdatesRequest request) {
        DCConstants.DCLogger.info("RecentUpdatesRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());
        session.dumpNewUpdates(conn, request);
    }

    @Override
    public void onReceive(RpcConnection conn, FastRecentUpdatesRequest request) {
        DCConstants.DCLogger.info("FastRecentUpdatesRequest client = " + request.getClientId());
        final ClientSession session = getSession( request.getClientId());
        session.dumpNewUpdates(conn, request);
    }
}

class ClientSession
{
    String clientId;
    Map<CRDTIdentifier,CRDTSessionInfo> subscriptions;
    Set<CRDTIdentifier> updates;
    Set<CRDTIdentifier> notifications;
    RpcConnection conn;
    
    ClientSession( String clientId) {
        this.clientId = clientId; 
        subscriptions = new TreeMap<CRDTIdentifier,CRDTSessionInfo>();
        updates = new TreeSet<CRDTIdentifier>();
        notifications = new TreeSet<CRDTIdentifier>();
        conn = null;
    }
    
    void subscribeUpdates( CRDTIdentifier id) {
        updates.add(id);
    }
    
    void unsubscribeUpdates( CRDTIdentifier id) {
        updates.remove(id);
    }
    
    void subscribeNotifications( CRDTIdentifier id) {
        notifications.add(id);
    }
    
    void unsubscribeNotifications( CRDTIdentifier id) {
        notifications.remove(id);
    }
    
    void dumpNewUpdates( RpcConnection conn, FastRecentUpdatesRequest request) {
       //TODO: return notifications
    }
    
    void dumpNewUpdates( RpcConnection conn, RecentUpdatesRequest request) {
        //TODO: return updates
    }
    
    public boolean equals( Object obj) {
        return obj instanceof ClientSession && ((ClientSession)obj).clientId.equals(clientId);
    }
    
    public int hashCode() {
        return clientId.hashCode();
    }
}

class CRDTSessionInfo
{
    CausalityClock oldClock;
    CausalityClock newClock;
    List<CRDTObjectOperationsGroup> updates;
    
}


class Reply implements RpcMessage {

    Reply() {
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        if (conn.expectingReply()) {
            ((Handler) handler).onReceive(conn, this);
        } else {
            ((Handler) handler).onReceive(this);
        }
    }
}
