package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Single server replying to client request. This class is used only to allow
 * client testing.
 * 
 * @author nmp
 * 
 */
public class DCServer {
    DCSurrogate server;
    String sequencerHost;

    protected DCServer(String sequencerHost) {
        this.sequencerHost = sequencerHost;
        init();
    }

    protected void init() {

    }

    public void startSurrogServer() {
        Sys.init();

        server = new DCSurrogate(Networking.Networking.rpcBind(DCConstants.SURROGATE_PORT, null), Networking.rpcBind(0,
                null), Networking.resolve(sequencerHost, DCConstants.SEQUENCER_PORT));
    }

    public static void main(String[] args) {
        new DCServer(args.length == 0 ? "localhost" : args[0]).startSurrogServer();
    }
}

class CRDTData<V extends CRDT<V>> {
    CRDT<V> crdt;
    CausalityClock clock;
    CausalityClock pruneClock;

    CRDTData(CRDT<V> crdt, CausalityClock clock, CausalityClock pruneClock) {
        this.crdt = crdt;
        this.clock = clock;
        this.pruneClock = pruneClock;
    }
}

class DCSurrogate extends Handler implements swift.client.proto.SwiftServer {

    RpcEndpoint endpoint;
    Endpoint sequencerServerEndpoint;
    RpcEndpoint sequencerClientEndpoint;
    Map<String, Map<String, CRDTData<?>>> database;

    CausalityClock version;

    DCSurrogate(RpcEndpoint e, RpcEndpoint seqClt, Endpoint seqSrv) {
        initData();
        this.endpoint = e;
        this.sequencerServerEndpoint = seqSrv;
        this.sequencerClientEndpoint = seqClt;
        this.endpoint.setHandler(this);
        System.out.println("Server ready...");
    }

    private void initData() {
        this.database = new HashMap<String, Map<String, CRDTData<?>>>();

        this.version = ClockFactory.newClock();

        IntegerVersioned i = new IntegerVersioned();
        i.setClock(version);
        i.setPruneClock(version);
        CRDTIdentifier id = new CRDTIdentifier("e", "1");
        putCRDT(id, i, i.getClock(), i.getPruneClock());

        IntegerVersioned i2 = new IntegerVersioned();
        i2.setClock(version);
        i2.setPruneClock(version);
        CRDTIdentifier id2 = new CRDTIdentifier("e", "2");
        putCRDT(id2, i2, i2.getClock(), i2.getPruneClock());

    }

    /**
     * Return null if CRDT does not exist
     */
    <V extends CRDT<V>> void putCRDT(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clk, CausalityClock prune) {
        // TODO:need to check prune stuff
        prune = ClockFactory.newClock();
        Map<String, CRDTData<?>> m;
        synchronized (database) {
            m = database.get(id.getTable());
            if (m == null) {
                m = new HashMap<String, CRDTData<?>>();
                database.put(id.getTable(), m);
            }
        }
        synchronized (m) {
            @SuppressWarnings("unchecked")
            CRDTData<V> data = (CRDTData<V>) m.get(id.getKey());
            if (data == null) {
                data = new CRDTData<V>(crdt, clk, prune);
                m.put(id.getKey(), data);
            } else {
                data.crdt.merge(crdt);
                data.clock.merge(clk);
                data.pruneClock.merge(prune);
            }
        }
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> boolean execCRDT(CRDTObjectOperationsGroup<V> grp) {
        CRDTIdentifier id = grp.getTargetUID();
        CRDTData<V> data = (CRDTData<V>) getCRDT(id, null);
        if (data == null)
            return false;
        Iterator<CRDTOperation<V>> it = grp.operations.iterator();
        while (it.hasNext()) {
            CRDTOperation<V> o = it.next();
            System.out.println("op:" + o);
            o.applyTo((V) data.crdt);
        }
        return true;
    }

    /**
     * Return null if CRDT does not exist
     * 
     * If clock equals to null, just return full CRDT
     */
    CRDTData<?> getCRDT(CRDTIdentifier id, CausalityClock clock) {
        Map<String, CRDTData<?>> m;
        synchronized (database) {
            m = database.get(id.getTable());
            if (m == null)
                return null;
        }
        synchronized (m) {
            CRDTData<?> crdt = m.get(id.getKey());
            return crdt;
        }
    }

    @Override
    public void onReceive(RpcConnection conn, FetchObjectVersionRequest request) {
        System.err.println("FetchObjectVersionRequest");

        // TODO: check received timevector

        CRDTData<?> crdt = getCRDT(request.getUid(), request.getVersion());
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
    public void onReceive(RpcConnection conn, FetchObjectDeltaRequest request) {
        System.err.println("FetchObjectDeltaRequest");

        // TODO: check received timevector

        CRDTData<?> crdt = getCRDT(request.getUid(), request.getVersion());
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
    public void onReceive(RpcConnection conn, CommitUpdatesRequest request) {
        System.err.println("CommitUpdatesRequest");
        List<CRDTObjectOperationsGroup<?>> ops = request.getObjectUpdateGroups();
        Timestamp ts = request.getBaseTimestamp();
        Iterator<CRDTObjectOperationsGroup<?>> it = ops.iterator();
        boolean ok = true;
        while (it.hasNext()) {
            CRDTObjectOperationsGroup<?> grp = it.next();
            ok = ok && execCRDT(grp);
        }
        synchronized (version) {
            version.record(ts);
        }

        if (ok) {
            conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.COMMITTED, ts));
        } else {
            conn.reply(new CommitUpdatesReply(CommitUpdatesReply.CommitStatus.INVALID_OPERATION, ts));
        }
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
        // TODO Auto-generated method stub

    }
}

class Reply implements RpcMessage {

    Reply() {
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        if (conn.expectingReply())
            ((Handler) handler).onReceive(conn, this);
        else
            ((Handler) handler).onReceive(this);
    }
}
