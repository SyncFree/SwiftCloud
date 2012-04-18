package swift.client;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.same;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesReply.CommitStatus;
import swift.client.proto.CommitUpdatesReplyHandler;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.client.proto.FetchObjectVersionReplyHandler;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampReplyHandler;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CachePolicy;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * SwiftImpl test using mock endpoint of a server, to drive test with
 * preprepared messages.
 * 
 * @author mzawirski
 */
public class SwiftImplTest extends EasyMockSupport {
    private RpcEndpoint mockLocalEndpoint;
    private Endpoint mockServerEndpoint;
    private SwiftImpl swiftImpl;
    private CausalityClock serverClock;
    private TimestampSource<Timestamp> serverTimestampGen;

    private CRDTIdentifier idCrdtA;
    private CRDTIdentifier idCrdtB;

    @Before
    public void setUp() {
        mockLocalEndpoint = createMock(RpcEndpoint.class);
        mockServerEndpoint = createMock(Endpoint.class);
        swiftImpl = new SwiftImpl(mockLocalEndpoint, mockServerEndpoint, new InfiniteObjectsCache());
        serverClock = ClockFactory.newClock();
        serverTimestampGen = new IncrementalTimestampGenerator("server");
    }

    @Test
    public void testSingleTxnCreateObject() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // Specify communication with the server mock.
        mockLocalEndpoint.send(same(mockServerEndpoint), isA(LatestKnownClockRequest.class),
                isA(LatestKnownClockReplyHandler.class));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
                ((LatestKnownClockReplyHandler) replyHandler).onReceive(null, new LatestKnownClockReply(serverClock));
                return true;
            }
        });
        mockLocalEndpoint.send(same(mockServerEndpoint), isA(FetchObjectVersionRequest.class),
                isA(FetchObjectVersionReplyHandler.class));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
                final FetchObjectVersionReply fetchReply = new FetchObjectVersionReply(FetchStatus.OBJECT_NOT_FOUND,
                        null, serverClock, null);
                ((FetchObjectVersionReplyHandler) replyHandler).onReceive(null, fetchReply);
                return true;
            }
        });
        final Timestamp txn1Timestamp = serverTimestampGen.generateNew();
        mockLocalEndpoint.send(same(mockServerEndpoint), isA(GenerateTimestampRequest.class),
                isA(GenerateTimestampReplyHandler.class));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
                final GenerateTimestampRequest request = (GenerateTimestampRequest) m;
                assertFalse(request.getClientId().isEmpty());
                assertNull(request.getPreviousTimestamp());
                assertEquals(CMP_CLOCK.CMP_EQUALS, ClockFactory.newClock().compareTo(request.getDominatedClock()));
                ((GenerateTimestampReplyHandler) replyHandler).onReceive(null, new GenerateTimestampReply(
                        txn1Timestamp, 1000));
                return true;
            }
        });

        mockLocalEndpoint.send(same(mockServerEndpoint), isA(CommitUpdatesRequest.class),
                isA(CommitUpdatesReplyHandler.class));
        expectLastCall().andDelegateTo(new DummyRpcEndpoint() {
            @Override
            public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
                final CommitUpdatesRequest request = (CommitUpdatesRequest) m;
                assertFalse(request.getClientId().isEmpty());
                assertEquals(txn1Timestamp, request.getBaseTimestamp());
                assertEquals(1, request.getObjectUpdateGroups().size());
                // Verify message integrity.
                assertEquals(request.getBaseTimestamp(), request.getObjectUpdateGroups().get(0).getBaseTimestamp());

                ((CommitUpdatesReplyHandler) replyHandler).onReceive(null, new CommitUpdatesReply(
                        CommitStatus.COMMITTED, request.getBaseTimestamp()));
                return true;
            }
        });
        replayAll();

        // Actual test: execute 1 transaction creating and updating object A.
        final TxnHandleImpl txn = swiftImpl.beginTxn(CachePolicy.MOST_RECENT, true);
        final IntegerTxnLocal crdtA = txn.get(idCrdtA, true, IntegerVersioned.class);
        assertEquals(new Integer(0), crdtA.getValue());
        crdtA.add(5);
        txn.commit();

        verifyAll();
    }

    @Ignore
    @Test
    public void testSingleTxnRetrievePreexistingObject() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // TODO
    }

    @Ignore
    @Test
    public void testTwoTxnsRefreshObject() {
        // TODO
    }

    @Ignore
    @Test
    public void testSingleTxnCacheObject() {
        // TODO
    }

    @Ignore
    @Test
    public void testConsistency() {
        // TODO
    }

    @Ignore
    @Test
    public void testQueuingUpLocalTransaction() {
        // TODO
    }

    private class DummyRpcEndpoint implements RpcEndpoint {
        @Override
        public Endpoint localEndpoint() {
            return null;
        }

        @Override
        public boolean send(Endpoint dst, RpcMessage m) {
            return false;
        }

        @Override
        public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
            return false;
        }

        @Override
        public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
            return false;
        }

        @Override
        public void setHandler(RpcHandler handler) {
        }
    }
}
