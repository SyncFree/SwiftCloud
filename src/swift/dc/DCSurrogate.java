package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
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
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.CausalityClock;
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

    RpcEndpoint endpoint;
    Endpoint sequencerServerEndpoint;
    RpcEndpoint sequencerClientEndpoint;
    DCDataServer dataServer;
    
    Map<String,ClientSession> sessions;   // map clientId -> clientSession
    Map<CRDTIdentifier,Set<String>> cltsObserving;   // map crdtIdentifier -> Set<clientId>

    DCSurrogate(RpcEndpoint e, RpcEndpoint seqClt, Endpoint seqSrv) {
        initData();
        this.endpoint = e;
        this.sequencerServerEndpoint = seqSrv;
        this.sequencerClientEndpoint = seqClt;
        this.endpoint.setHandler(this);
        System.out.println("Server ready...");
    }

    private void initData() {
        sessions = new HashMap<String,ClientSession>();
        cltsObserving = new HashMap<CRDTIdentifier,Set<String>>();
        dataServer = new DCDataServer();
    }
    
    void addToObserving(CRDTIdentifier id, String cltId) {
        synchronized (cltsObserving) {
            Set<String> clts = cltsObserving.get(id);
            if (clts == null) {
                clts = new TreeSet<String>();
                cltsObserving.put(id, clts);
            }
            clts.add(cltId);
        }
    }

    void remFromObserving(CRDTIdentifier id, String cltId) {
        synchronized (cltsObserving) {
            Set<String> clts = cltsObserving.get(id);
            if (clts != null) {
                cltsObserving.remove(cltId);
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
                session = new ClientSession();
                sessions.put(clientId, session);
            }
            return session;
        }
    }
    
    void notifyNewUpdates( CRDTObjectOperationsGroup updates) {
        
    }
    
    /**
     * Return null if CRDT does not exist
     */
    <V extends CRDT<V>> CRDTData<V> putCRDT(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clk, CausalityClock prune) {
        return dataServer.putCRDT(id, crdt, clk, prune);
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> boolean execCRDT(CRDTObjectOperationsGroup<V> grp, CausalityClock version) {
        return dataServer.execCRDT(grp, version);
    }

    /**
     * Return null if CRDT does not exist
     * 
     * If clock equals to null, just return full CRDT
     * @param subscribe 
     */
    CRDTData<?> getCRDT(CRDTIdentifier id, CausalityClock clock, boolean subscribe) {
        return dataServer.getCRDT(id, clock, subscribe);
    }

    @Override
    public void onReceive(RpcConnection conn, FetchObjectVersionRequest request) {
        System.err.println("FetchObjectVersionRequest");
        
        // TODO: check received timevector

        CRDTData<?> crdt = getCRDT(request.getUid(), request.getVersion(), request.isSubscribeUpdatesRequest());
        if (crdt == null) {
            conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null, request
                    .getVersion(), ClockFactory.newClock()));
        } else {
            ClientSession session = getSession(request.getClientId());
            synchronized (session) {
            }
            addToObserving(request.getUid(), request.getClientId());
            synchronized (crdt) {
                conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OK, crdt.crdt, crdt.clock,
                        crdt.pruneClock));
            }
        }
    }

    @Override
    public void onReceive(RpcConnection conn, FetchObjectDeltaRequest request) {
        System.err.println("FetchObjectDeltaRequest");

        // TODO: check received timevector

        CRDTData<?> crdt = getCRDT(request.getUid(), request.getVersion(), request.isSubscribeUpdatesRequest());
        if (crdt == null) {
            conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND, null, request
                    .getVersion(), ClockFactory.newClock()));
        } else {
            synchronized (crdt) {
                conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OK, crdt.crdt, crdt.clock,
                        crdt.pruneClock));
            }
        }

    }

    @Override
    public void onReceive(final RpcConnection conn, GenerateTimestampRequest request) {
        System.err.println("GenerateTimestampRequest");
        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new GenerateTimestampReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, GenerateTimestampReply reply) {
                System.err.println("GenerateTimestampRequest: forwarding reply");
                conn.reply(reply);
            }
        });
    }

    @Override
    public void onReceive(final RpcConnection conn, KeepaliveRequest request) {
        System.err.println("KeepaliveRequest");
        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new KeepaliveReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, KeepaliveReply reply) {
                System.err.println("KeepaliveRequest: forwarding reply");
                conn.reply(reply);
            }
        });
    }

    @Override
    public void onReceive(final RpcConnection conn, CommitUpdatesRequest request) {
        System.err.println("CommitUpdatesRequest");
        List<CRDTObjectOperationsGroup<?>> ops = request.getObjectUpdateGroups();
        final Timestamp ts = request.getBaseTimestamp();
        final CausalityClock version = ClockFactory.newClock();
        Iterator<CRDTObjectOperationsGroup<?>> it = ops.iterator();
        boolean ok = true;
        while (it.hasNext()) {
            CRDTObjectOperationsGroup<?> grp = it.next();
            ok = ok && execCRDT(grp, version);
        }
        synchronized (version) {
            version.record(ts);
        }
        final boolean txResult = ok;
        // TODO: handle failure

        sequencerClientEndpoint.send(sequencerServerEndpoint, new CommitTSRequest(ts, version, ok),
                new CommitTSReplyHandler() {
                    @Override
                    public void onReceive(RpcConnection conn0, CommitTSReply reply) {
                        System.err.println("Commit: received CommitTSRequest");
                        if (txResult && reply.getStatus() == CommitTSReply.CommitTSStatus.OK) {
                            conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.COMMITTED, ts));
                        } else {
                            conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.INVALID_OPERATION, ts));
                        }
                    }
                });
    }

    @Override
    public void onReceive(final RpcConnection conn, LatestKnownClockRequest request) {
        System.err.println("LatestKnownClockRequest");
        sequencerClientEndpoint.send(sequencerServerEndpoint, request, new LatestKnownClockReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn0, LatestKnownClockReply reply) {
                System.err.println("LatestKnownClockRequest: forwarding reply");
                conn.reply(reply);
            }
        });
    }

    @Override
    public void onReceive(RpcConnection conn, UnsubscribeUpdatesRequest request) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onReceive(RpcConnection conn, RecentUpdatesRequest request) {
        ClientSession session = getSession(request.getClientId());
        synchronized (session) {
            
        }
    }
}

class ClientSession
{
    Map<CRDTIdentifier, CausalityClock> newlyConfirmedSubscriptions;
    List<CRDTObjectOperationsGroup> updates;
    CausalityClock clock;
    RpcConnection conn;
    
    ClientSession() {
        newlyConfirmedSubscriptions = new TreeMap<CRDTIdentifier,CausalityClock>();
        updates = new ArrayList<CRDTObjectOperationsGroup>();
        clock = null;
        conn = null;
    }
    
    
    void dumpNewUpdates( RpcConnection conn, RecentUpdatesRequest request) {
        
/*        request.
        
        
        conn.reply(new FetchObjectVersionReply(FetchObjectVersionReply.FetchStatus.OK, crdt.crdt, crdt.clock,
                crdt.pruneClock));
        
*/    }
    
    
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
