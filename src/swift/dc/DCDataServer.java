package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import swift.client.proto.SubscriptionType;
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
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
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
 * Class to maintain data in the server.
 * 
 * @author preguica
 */
class DCDataServer {

    Map<String, Map<String, CRDTData<?>>> database;

    CausalityClock version;

    DCDataServer() {
        initData();
        System.out.println("Data server ready...");
    }

    private void initData() {
        this.database = new HashMap<String, Map<String, CRDTData<?>>>();

        this.version = ClockFactory.newClock();

        IntegerVersioned i = new IntegerVersioned();
        CRDTIdentifier id = new CRDTIdentifier("e", "1");
        i.init(id, version, version, true);
        putCRDT(id, i, i.getClock(), i.getPruneClock());

        IntegerVersioned i2 = new IntegerVersioned();
        CRDTIdentifier id2 = new CRDTIdentifier("e", "2");
        i2.init(id2, version, version, true);
        putCRDT(id2, i2, i2.getClock(), i2.getPruneClock());

    }

    /**
     * Return null if CRDT does not exist
     */
    <V extends CRDT<V>> CRDTData<V> putCRDT(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clk, CausalityClock prune) {
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
            return data;
        }
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> boolean execCRDT(CRDTObjectOperationsGroup<V> grp, CausalityClock version) {
        CRDTIdentifier id = grp.getTargetUID();
        CRDTData<?> data = getCRDT(id, null, SubscriptionType.NONE);
        if (data == null) {
            if (!grp.hasCreationState()) {
                return false;
            }
            CRDT crdt = grp.getCreationState();
            CausalityClock clk = grp.getDependency();
            if (clk == null) {
                clk = ClockFactory.newClock();
            }
            CausalityClock prune = ClockFactory.newClock();
            crdt.init(id, clk, prune, true);
            data = putCRDT(id, crdt, clk, prune); // will merge if object exists
        }

        synchronized (data) {
            // Assumption: dependencies are checked at sequencer level, since
            // causality and dependencies are given at inter-object level.
            data.crdt.execute((CRDTObjectOperationsGroup) grp, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
        }
        return true;
    }

    /**
     * Return null if CRDT does not exist
     * 
     * If clock equals to null, just return full CRDT
     * @param subscribe 
     */
    CRDTData<?> getCRDT(CRDTIdentifier id, CausalityClock clock, SubscriptionType subscriptionType) {
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

