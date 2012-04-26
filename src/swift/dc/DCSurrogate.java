package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.client.proto.FastRecentUpdatesReply.SubscriptionStatus;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
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
import swift.dc.proto.DHTSendNotification;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.pubsub.PubSub;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica
 */
class DCSurrogate extends Handler implements swift.client.proto.SwiftServer, PubSub.Handler {

    String surrogateId;
    RpcEndpoint endpoint;
    Endpoint sequencerServerEndpoint;
    RpcEndpoint sequencerClientEndpoint;
    DCDataServer dataServer;
    CausalityClock estimatedDCVersion;       // estimate of current DC state
    
    Map<String,ClientPubInfo> sessions;   // map clientId -> ClientPubInfo
    Map<CRDTIdentifier,Map<String,ClientPubInfo>> cltsObserving;   // map crdtIdentifier -> Map<clientId,ClientPubInfo>

    DCSurrogate(RpcEndpoint e, RpcEndpoint seqClt, Endpoint seqSrv, Properties props) {
        this.surrogateId = "s" + System.nanoTime();
        this.endpoint = e;
        this.sequencerServerEndpoint = seqSrv;
        this.sequencerClientEndpoint = seqClt;
        initData( props);
        this.endpoint.setHandler(this);
        DCConstants.DCLogger.info("Server ready...");
    }

    private void initData( Properties props) {
        sessions = new HashMap<String,ClientPubInfo>();
        cltsObserving = new HashMap<CRDTIdentifier,Map<String,ClientPubInfo>>();
        estimatedDCVersion = ClockFactory.newClock();
        dataServer = new DCDataServer( this, props);
    }
    
    String getId() {
        return surrogateId;
    }
    
    CausalityClock getEstimatedDCVersionCopy() {
        synchronized (estimatedDCVersion) {
            return estimatedDCVersion.clone();
        }
    }
    
    /********************************************************************************************
     * Methods related with notifications from clients 
     *******************************************************************************************/
    
    /**
     * Add client to start observing changes on CRDT id
     * @param observing fi true, client wants to receive updated; otherwise, notifications
     */
    void addToObserving(CRDTIdentifier id, boolean observing, CausalityClock clk, ClientPubInfo session) {
        synchronized (cltsObserving) {
            Map<String,ClientPubInfo> clts = cltsObserving.get(id);
            if (clts == null) {
                clts = new TreeMap<String,ClientPubInfo>();
                cltsObserving.put(id, clts);
            }
            if( clts.size() == 0)
                PubSub.PubSub.subscribe(id.toString(), this);
            clts.put(session.getClientId(), session);
            if( observing)
                session.setObserving( clk, id, true);
            else
                session.setNotificating( clk, id, true);
        }
    }

    /**
     * Removes client from observing changes on CRDT id
     */
    void remFromObserving(CRDTIdentifier id, ClientPubInfo session) {
        synchronized (cltsObserving) {
                Map<String,ClientPubInfo> clts = cltsObserving.get(id);
                if( clts == null) {
                    PubSub.PubSub.unsubscribe(id.toString(), this);
                    return;
                }
                clts.remove(session.getClientId());
                if( clts.size() == 0) {
                    PubSub.PubSub.unsubscribe(id.toString(), this);
                    cltsObserving.remove(id);
                }
        }
    }

    @Override
    public void notify(String group, Object payload) {
        final DHTSendNotification notification = (DHTSendNotification)payload;
        final CRDTIdentifier id = notification.getInfo().getId();
        DCConstants.DCLogger.info( "Surrogate: Notify new updates for:" + notification.getInfo().getId());
        
        synchronized (cltsObserving) {
            Map<String,ClientPubInfo> map = cltsObserving.get( id);
            if( map == null)
                return;
            Iterator<Entry<String,ClientPubInfo>> it = map.entrySet().iterator();
            while( it.hasNext()) {
                Entry<String,ClientPubInfo> entry = it.next();
                entry.getValue().addNotifications( notification.getInfo(), getEstimatedDCVersionCopy());
            }
        }
    }

    ClientPubInfo getSession(String clientId) {
        synchronized (sessions) {
            ClientPubInfo session = sessions.get(clientId);
            if (session == null) {
                session = new ClientPubInfo( clientId);
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
        return dataServer.getCRDT(id, subscribe);    // call DHT server
    }

    @Override
    public void onReceive(RpcConnection conn, FetchObjectVersionRequest request) {
        DCConstants.DCLogger.info("FetchObjectVersionRequest client = " + request.getClientId());
        conn.reply(handleFetchVersionRequest(request));
    }
    
    @Override
    public void onReceive(RpcConnection conn, FetchObjectDeltaRequest request) {
        DCConstants.DCLogger.info("FetchObjectDeltaRequest client = " + request.getClientId());
        conn.reply(handleFetchVersionRequest(request));
    }

    private FetchObjectVersionReply handleFetchVersionRequest(FetchObjectVersionRequest request) {
        DCConstants.DCLogger.info("FetchObjectVersionRequest client = " + request.getClientId() + "; crdt id = "
                + request.getUid());
        final ClientPubInfo session = getSession( request.getClientId());

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

        CRDTObject<?> crdt = getCRDT(request.getUid(), request.getSubscriptionType());
        if (crdt == null) {
            return new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null, estimatedDCVersionCopy,
                    ClockFactory.newClock(), estimatedDCVersionCopy);
        } else {
            if (request.getSubscriptionType() != SubscriptionType.NONE) {
                if (request.getSubscriptionType() == SubscriptionType.NOTIFICATION)
                    addToObserving(request.getUid(), false, crdt.clock.clone(), session);
                else if (request.getSubscriptionType() == SubscriptionType.UPDATES)
                    addToObserving(request.getUid(), true, crdt.clock.clone(), session);
            }
            synchronized (crdt) {
                crdt.clock.merge(estimatedDCVersionCopy);
                final FetchObjectVersionReply.FetchStatus status = (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND
                        : FetchStatus.OK;
                return new FetchObjectVersionReply(status, crdt.crdt, crdt.clock, crdt.pruneClock, estimatedDCVersionCopy);
            }
        }
    }

    @Override
    public void onReceive(final RpcConnection conn, GenerateTimestampRequest request) {
        DCConstants.DCLogger.info("GenerateTimestampRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession( request.getClientId());

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
        final ClientPubInfo session = getSession( request.getClientId());

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
        final ClientPubInfo session = getSession( request.getClientId());

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
        sequencerClientEndpoint.send(sequencerServerEndpoint, new CommitTSRequest(ts, estimatedDCVersionCopy, ok, 
                        request.getObjectUpdateGroups(), request.getBaseTimestamp()),
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
        final ClientPubInfo session = getSession( request.getClientId());
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
        final ClientPubInfo session = getSession( request.getClientId());
        remFromObserving(request.getUid(), session);
    }

    @Override
    public void onReceive(RpcConnection conn, RecentUpdatesRequest request) {
        DCConstants.DCLogger.info("RecentUpdatesRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession( request.getClientId());
        session.dumpNewUpdates(conn, request);
    }

    @Override
    public void onReceive(RpcConnection conn, FastRecentUpdatesRequest request) {
        DCConstants.DCLogger.info("FastRecentUpdatesRequest client = " + request.getClientId());
        final ClientPubInfo session = getSession( request.getClientId());
        session.dumpNewUpdates(conn, request, getEstimatedDCVersionCopy());
    }

}

class ClientPubInfo
{
    private String clientId;
    boolean hasUpdates;
    RpcConnection conn;
    long requestTime;
    private Map<CRDTIdentifier,CRDTSessionInfo> subscriptions;

    ClientPubInfo() {
    }
    public ClientPubInfo(String clientId) {
        this.clientId = clientId;
        hasUpdates = false;
        conn = null;
        subscriptions = new TreeMap<CRDTIdentifier,CRDTSessionInfo>();
    }
    public synchronized boolean isObserving( CRDTIdentifier id) {
        CRDTSessionInfo info = subscriptions.get(id);
        if( info == null)
            return false;
        else
            return info.isObserving();
    }
    public synchronized boolean isNotificating( CRDTIdentifier id) {
        CRDTSessionInfo info = subscriptions.get(id);
        if( info == null)
            return false;
        else
            return info.isNotificating();
    }
    public synchronized boolean isNotificatingOrObserving(CRDTIdentifier id) {
        CRDTSessionInfo info = subscriptions.get(id);
        if( info == null)
            return false;
        else
            return info.isObserving() || info.isNotificating();
    }
    public synchronized void setObserving( CausalityClock clk, CRDTIdentifier id, boolean set) {
        CRDTSessionInfo info = subscriptions.get(id);
        if( info == null) {
            if( ! set)
                return;
            info = new CRDTSessionInfo( clk, true, false);
            subscriptions.put(id, info);
        }
        info.setObserving(set);
        if( ! info.isObserving() && ! info.isNotificating()) {
            subscriptions.remove( id);
        }
    }
    public synchronized void setNotificating( CausalityClock clk, CRDTIdentifier id, boolean set) {
        CRDTSessionInfo info = subscriptions.get(id);
        if( info == null) {
            if( ! set)
                return;
            info = new CRDTSessionInfo( clk, false, true);
            subscriptions.put(id, info);
        }
        info.setNotificating(set);
        if( ! info.isObserving() && ! info.isNotificating()) {
            subscriptions.remove( id);
        }
    }
    synchronized void dumpNewUpdates( RpcConnection conn, FastRecentUpdatesRequest request, CausalityClock clk) {
        this.conn = conn;
        requestTime = System.currentTimeMillis();
        if( hasUpdates)
            dumpNotifications( clk);
    }
     
    synchronized void dumpNewUpdates( RpcConnection conn, RecentUpdatesRequest request) {
         //TODO: return updates
    }
    public synchronized void addNotifications(ObjectSubscriptionInfo info, CausalityClock clk) {
         CRDTSessionInfo session = subscriptions.get(info.getId());
         if( session == null)
             return;
         session.addNotifications( info);
         hasUpdates = true;
         dumpNotifications( clk);
    }
    
    private synchronized void dumpNotifications( CausalityClock clk) {
        if( conn == null)
            return;
        List<ObjectSubscriptionInfo> notifications = new ArrayList<ObjectSubscriptionInfo>();
        SubscriptionStatus status = subscriptions.size() == 0 ? SubscriptionStatus.LOST : SubscriptionStatus.ACTIVE;
        Iterator<Entry<CRDTIdentifier,CRDTSessionInfo>> it = subscriptions.entrySet().iterator();
        while( it.hasNext()) {
            Entry<CRDTIdentifier,CRDTSessionInfo> entry = it.next();
            entry.getValue().addSubscriptionInfo(entry.getKey(),notifications);
        }
        conn.reply( new FastRecentUpdatesReply( status, notifications, clk));
        
        hasUpdates = false;
        conn = null;
    }

    public String getClientId() {
        return clientId;
    }
}

/*
class ClientSession
{
    String clientId;
    Map<CRDTIdentifier,CRDTSessionInfo> subscriptions;
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
    
    void addNotifications(ObjectSubscriptionInfo info) {
        
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
*/

class CRDTSessionInfo
{
    private boolean observing;
    private boolean notificating;
    private boolean hasChanges;
    private CausalityClock oldClock;
    private CausalityClock newClock;
    private List<CRDTObjectOperationsGroup<?>> updates;

    public CRDTSessionInfo(CausalityClock clk, boolean observing, boolean notificating) {
        this.oldClock = clk;
        this.newClock = clk.clone();
        this.observing = observing;
        this.notificating = notificating;
        this.hasChanges = false;
        updates = new ArrayList<CRDTObjectOperationsGroup<?>>();
    }

    public void addSubscriptionInfo(CRDTIdentifier id, List<ObjectSubscriptionInfo> notifications) {
        if( ! hasChanges) {
            notifications.add(new ObjectSubscriptionInfo(id, oldClock, newClock, updates, false));
        } else {
            notifications.add(new ObjectSubscriptionInfo(id, oldClock, newClock, updates, true));
        }
        hasChanges = false;
        newClock = oldClock;
        updates.clear();
    }

    public void addNotifications(ObjectSubscriptionInfo info) {
        if( ! hasChanges) {
            oldClock = info.getOldClock();
            newClock = info.getNewClock();
            updates.addAll(info.getUpdates());
            hasChanges = true;
        } else {
            //TODO: check if new clock == old clock
            newClock.merge( info.getNewClock());
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
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        if (conn.expectingReply()) {
            ((Handler) handler).onReceive(conn, this);
        } else {
            ((Handler) handler).onReceive(this);
        }
    }
}
