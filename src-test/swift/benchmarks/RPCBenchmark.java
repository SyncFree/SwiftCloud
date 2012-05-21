package swift.benchmarks;

import static org.junit.Assert.assertEquals;
import static sys.net.api.Networking.Networking;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.client.proto.FetchObjectVersionReplyHandler;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.KeepaliveRequest;
import swift.client.proto.LatestKnownClockRequest;
import swift.client.proto.RecentUpdatesRequest;
import swift.client.proto.SubscriptionType;
import swift.client.proto.SwiftServer;
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.crdt.operations.IntegerUpdate;
import swift.dc.proto.SeqCommitUpdatesRequest;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcMessage;

import static sys.net.api.Networking.*;

/**
 * Simple RPC+Kryo benchmark.
 * 
 * @author mzawirski
 */
public class RPCBenchmark {

    private static final int SERVER_PORT = 8001;

    private RpcEndpoint serverEndpoint;
    private RpcEndpoint clientEndpoint;
    private Endpoint clientToServerEndpoint;

    private final static CRDTIdentifier objectId = new CRDTIdentifier("test", "obj");
    private final static CausalityClock emptyClock = ClockFactory.newClock();
    private CausalityClock causalityClock1;

    private IntegerVersioned integer;

    @Before
    public void setUp() throws Exception {
        Sys.init();
        // final Kryo kryo = ((KryoSerializer)
        // Networking.Networking.serializer()).kryo();
        // kryo.register(TestRequest.class);
        // kryo.register(TestReply.class);

        serverEndpoint = Networking.rpcBind(SERVER_PORT, TransportProvider.DEFAULT).toService(0, new RpcServer() );
        clientEndpoint = Networking.rpcConnect().toDefaultService();
        clientToServerEndpoint = Networking.resolve("localhost", SERVER_PORT);

        causalityClock1 = ClockFactory.newClock();
        final Timestamp site1Timestamp = new IncrementalTimestampGenerator("site1").generateNew();
        causalityClock1.record(site1Timestamp);
        final Timestamp site2Timestamp = new IncrementalTimestampGenerator("site2").generateNew();
        causalityClock1.record(site2Timestamp);

        integer = new IntegerVersioned();
        integer.init(objectId, causalityClock1, ClockFactory.newClock(), true);
        final CRDTObjectOperationsGroup<IntegerVersioned> opGroup1 = new CRDTObjectOperationsGroup<IntegerVersioned>(
                objectId, site1Timestamp, null);
        opGroup1.append(new IntegerUpdate(new IncrementalTripleTimestampGenerator(site1Timestamp).generateNew(), 1));
        opGroup1.setDependency(ClockFactory.newClock());
        integer.execute(opGroup1, CRDTOperationDependencyPolicy.RECORD_BLINDLY);

        final CRDTObjectOperationsGroup<IntegerVersioned> opGroup2 = new CRDTObjectOperationsGroup<IntegerVersioned>(
                objectId, site2Timestamp, null);
        opGroup2.append(new IntegerUpdate(new IncrementalTripleTimestampGenerator(site2Timestamp).generateNew(), 1));
        opGroup2.setDependency(ClockFactory.newClock());
        integer.execute(opGroup2, CRDTOperationDependencyPolicy.RECORD_BLINDLY);

        System.out.println("Waiting 10s...");
        // (to select JVM using VisualVM profiler)
        try {
            Thread.sleep(1000);
        } catch (InterruptedException x) {
        }
        System.out.println("Starting the test...");
    }

    @After
    public void tearDown() throws Exception {
        // TODO: how to close this stuff?
    }

    @Test
    public void test1000RPC() {
        final AtomicLong receivedAcks = new AtomicLong();
        final FetchObjectVersionReplyHandler countingReplyHandler = new FetchObjectVersionReplyHandler() {
            @Override
            public void onReceive(RpcHandle conn, FetchObjectVersionReply reply) {
                receivedAcks.incrementAndGet();
            }
        };

        final long startTime = System.currentTimeMillis();
        final int RPCS_NUMBER = 20000;
        for (int i = 0; i < RPCS_NUMBER; i++) {
            clientEndpoint.send(clientToServerEndpoint, new FetchObjectVersionRequest("client", objectId,
                    causalityClock1, true, SubscriptionType.NONE), countingReplyHandler);
        }
        assertEquals(RPCS_NUMBER, receivedAcks.get());
        final long duration = System.currentTimeMillis() - startTime;
        System.out.println("10000 ping RPCs executed in " + duration + "ms");
    }

    private class RpcServer implements SwiftServer {
        @Override
        public void onReceive(RpcHandle conn, FetchObjectVersionRequest request) {
            conn.reply(new FetchObjectVersionReply(FetchStatus.OK, integer, causalityClock1, emptyClock,
                    causalityClock1));
        }

        @Override
        public void onReceive(RpcHandle conn, GenerateTimestampRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, KeepaliveRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, LatestKnownClockRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, SeqCommitUpdatesRequest request) {
        }

        @Override
        public void onReceive(RpcMessage m) {
        }

        @Override
        public void onFailure(RpcHandle h) {
        }

        @Override
        public void onReceive(RpcHandle conn, FetchObjectDeltaRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, UnsubscribeUpdatesRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, RecentUpdatesRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, FastRecentUpdatesRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, CommitUpdatesRequest request) {
        }

        @Override
        public void onReceive(RpcHandle conn, RpcMessage m) {
        }
    }

    // private static class TestRequest implements RpcMessage {
    // protected int test;
    //
    // public TestRequest() {
    // }
    //
    // public TestRequest(int test) {
    // this.test = test;
    // }
    //
    // @Override
    // public void deliverTo(RpcHandle conn, RpcHandler handler) {
    // handler.onReceive(conn, this);
    // }
    // }
    //
    // private static class TestReply implements RpcMessage {
    // protected int test;
    //
    // public TestReply() {
    // }
    //
    // public TestReply(int test) {
    // this.test = test;
    // }
    //
    // @Override
    // public void deliverTo(RpcHandle conn, RpcHandler handler) {
    // handler.onReceive(this);
    // }
    // }
}
