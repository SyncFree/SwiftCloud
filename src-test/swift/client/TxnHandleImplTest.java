package swift.client;

import static org.easymock.EasyMock.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Test of TxnHandleImpl in isolation, using mocks/stubs of TxnManager, a much
 * simplified replacement. It allows us to cover some concurrency-related cases
 * and isolate from TxnManager implementation (bugs).
 * 
 * @author mzawirski
 */
public class TxnHandleImplTest extends EasyMockSupport {

    private TxnManager mockManager;
    private CausalityClock globalCausalityClock;
    private IncrementalTimestampGenerator localTimestampGen;
    private IncrementalTimestampGenerator globalTimestampGen;

    private IntegerVersioned crdtA;
    private CRDTIdentifier idCrdtA;
    private IntegerVersioned crdtB;
    private CRDTIdentifier idCrdtB;

    private TxnHandleImpl txn1;
    private TxnHandleImpl txn2;

    @Before
    public void setUp() {
        mockManager = createStrictMock(TxnManager.class);
        globalCausalityClock = ClockFactory.newClock();
        localTimestampGen = new IncrementalTimestampGenerator("client");
        globalTimestampGen = new IncrementalTimestampGenerator("server");

        crdtA = new IntegerVersioned();
        crdtB = new IntegerVersioned();
        idCrdtA = new CRDTIdentifier("x", "a");
        idCrdtB = new CRDTIdentifier("x", "b");
        crdtA.init(idCrdtA, globalCausalityClock, globalCausalityClock, true);
        crdtB.init(idCrdtB, globalCausalityClock, globalCausalityClock, false);

        txn1 = null;
        txn2 = null;
    }

    @Test
    public void testInitTxn() {
        txn1 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.EMPTY_LIST,
                localTimestampGen.generateNew());

        assertEquals(TxnStatus.PENDING, txn1.getStatus());
        assertNotNull(txn1.getLocalTimestamp());
        assertNull(txn1.getGlobalTimestamp());

        assertTrue(txn1.getLocalVisibleTransactions().isEmpty());
        final CausalityClock clockAll = txn1.getAllVisibleTransactionsClock();
        final CausalityClock clockGlobal = txn1.getGlobalVisibleTransactionsClock();
        assertNotNull(clockAll);
        assertEquals(CMP_CLOCK.CMP_EQUALS, clockAll.compareTo(clockGlobal));

        assertTrue(txn1.getAllLocalOperations().isEmpty());
        assertTrue(txn1.getAllGlobalOperations().isEmpty());
    }

    @Test
    public void testRollbackTxn() {
        // Specify expected TxnManager calls.
        mockManager.discardTxn(txn1Matcher());
        replayAll();

        // Actual test using mocks.
        txn1 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.EMPTY_LIST,
                localTimestampGen.generateNew());
        txn1.rollback();
        assertEquals(TxnStatus.CANCELLED, txn1.getStatus());

        // Verify mock calls.
        verifyAll();
    }

    @Test
    public void testSingleReadOnlyTxn() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // Specify calls to manager made by txn1.
        EasyMock.expect(mockManager.getObjectTxnView(txn1Matcher(), eq(idCrdtA), eq(true), eq(IntegerVersioned.class)))
                .andDelegateTo(new ObjectProviderManagerStub(crdtA));
        EasyMock.expect(mockManager.getObjectTxnView(txn1Matcher(), eq(idCrdtB), eq(false), eq(IntegerVersioned.class)))
                .andThrow(new NoSuchObjectException("object B not found"));
        mockManager.commitTxn(txn1Matcher());
        EasyMock.expectLastCall().andDelegateTo(new CommitingManagerStub(null, true));
        replayAll();

        // Actual test.
        txn1 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.EMPTY_LIST,
                localTimestampGen.generateNew());

        // Query for existing object A.
        final IntegerTxnLocal crdtAView = txn1.get(idCrdtA, true, IntegerVersioned.class);
        assertEquals(new Integer(0), crdtAView.getValue());
        // Query for nonexistent object B.
        try {
            txn1.get(idCrdtB, false, IntegerVersioned.class);
            fail();
        } catch (NoSuchObjectException x) {
        }

        assertTrue(txn1.isReadOnly());
        assertTrue(txn1.getLocalVisibleTransactions().isEmpty());
        assertTrue(txn1.getAllLocalOperations().isEmpty());

        txn1.commit();
        assertEquals(TxnStatus.COMMITTED_GLOBAL, txn1.getStatus());

        // Verify mock calls.
        verifyAll();
    }

    @Test
    public void testSingleReadOnlyTxnAsyncCommit() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // Specify calls to manager made by txn1.
        mockManager.commitTxn(txn1Matcher());
        EasyMock.expectLastCall().andDelegateTo(new CommitingManagerStub(null, false));
        replayAll();

        // Actual test.
        txn1 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.EMPTY_LIST,
                localTimestampGen.generateNew());
        final AtomicBoolean committedFlag = new AtomicBoolean();
        txn1.commitAsync(new CommitListener() {
            @Override
            public void onGlobalCommit(TxnHandle transaction) {
                committedFlag.set(true);
            }
        });
        assertEquals(TxnStatus.COMMITTED_LOCAL, txn1.getStatus());
        txn1.markGloballyCommitted();

        // Quick and dirty sleep() to verify the expected behavior as simply as
        // possible.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
        assertTrue(committedFlag.get());

        // Verify mock calls.
        verifyAll();
    }

    @Test
    public void testSingleUpdateTxnCreationAndUpdate() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // Specify calls to manager made by txn1.
        EasyMock.expect(mockManager.getObjectTxnView(txn1Matcher(), eq(idCrdtA), eq(true), eq(IntegerVersioned.class)))
                .andDelegateTo(new ObjectProviderManagerStub(crdtA));
        EasyMock.expect(mockManager.getObjectTxnView(txn1Matcher(), eq(idCrdtB), eq(true), eq(IntegerVersioned.class)))
                .andDelegateTo(new ObjectProviderManagerStub(crdtB));
        final Timestamp txn1GlobalTimestamp = globalTimestampGen.generateNew();
        mockManager.commitTxn(txn1Matcher());
        EasyMock.expectLastCall().andDelegateTo(new CommitingManagerStub(txn1GlobalTimestamp, true));
        replayAll();

        // Actual test.
        txn1 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.EMPTY_LIST,
                localTimestampGen.generateNew());

        // Perform some operations on existing object A.
        final IntegerTxnLocal crdtAView = txn1.get(idCrdtA, true, IntegerVersioned.class);
        crdtAView.add(1);
        // Try caching mechanism.
        final IntegerTxnLocal crdtAViewCached = txn1.get(idCrdtA, true, IntegerVersioned.class);
        assertEquals(new Integer(1), crdtAViewCached.getValue());
        crdtAViewCached.add(3);

        // Create object B.
        final IntegerTxnLocal crdtBView = txn1.get(idCrdtB, true, IntegerVersioned.class);
        assertEquals(new Integer(0), crdtBView.getValue());

        assertFalse(txn1.isReadOnly());
        assertTrue(txn1.getLocalVisibleTransactions().isEmpty());

        // Commit globally.
        txn1.commit();
        assertEquals(TxnStatus.COMMITTED_GLOBAL, txn1.getStatus());
        assertEquals(txn1GlobalTimestamp, txn1.getGlobalTimestamp());

        final CRDTObjectOperationsGroup<?> localOpsA = txn1.getObjectLocalOperations(idCrdtA);
        assertEquals(idCrdtA, localOpsA.getTargetUID());
        assertEquals(txn1.getLocalTimestamp(), localOpsA.getBaseTimestamp());
        assertFalse(localOpsA.hasCreationState());
        assertEquals(2, localOpsA.getOperations().size());

        final CRDTObjectOperationsGroup<?> localOpsB = txn1.getObjectLocalOperations(idCrdtB);
        assertEquals(idCrdtB, localOpsB.getTargetUID());
        assertEquals(txn1.getLocalTimestamp(), localOpsB.getBaseTimestamp());
        assertTrue(localOpsB.hasCreationState());
        assertNotNull(localOpsB.getCreationState());
        assertEquals(0, localOpsB.getOperations().size());

        final Collection<CRDTObjectOperationsGroup<?>> localOps = txn1.getAllLocalOperations();
        assertEquals(2, localOps.size());
        assertTrue(localOps.contains(localOpsA));
        assertTrue(localOps.contains(localOpsB));

        final Collection<CRDTObjectOperationsGroup<?>> globalOps = txn1.getAllGlobalOperations();
        assertEquals(2, globalOps.size());
        for (final CRDTObjectOperationsGroup<?> op : globalOps) {
            assertEquals(txn1.getGlobalTimestamp(), op.getBaseTimestamp());
        }

        // Verify mock calls.
        verifyAll();
    }

    @Test
    public void testTwoUpdateTxnsLocalDependency() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // Calls made by txn1.
        EasyMock.expect(mockManager.getObjectTxnView(txn1Matcher(), eq(idCrdtA), eq(true), eq(IntegerVersioned.class)))
                .andDelegateTo(new ObjectProviderManagerStub(crdtA));
        final Timestamp txn1GlobalTimestampProposal = globalTimestampGen.generateNew();
        final Timestamp txn1GlobalTimestampFinal = globalTimestampGen.generateNew();
        mockManager.commitTxn(txn1Matcher());
        EasyMock.expectLastCall().andDelegateTo(new CommitingManagerStub(txn1GlobalTimestampProposal, false));

        // Calls made by txn2.
        EasyMock.expect(mockManager.getObjectTxnView(txn2Matcher(), eq(idCrdtA), eq(true), eq(IntegerVersioned.class)))
                .andDelegateTo(new ObjectProviderManagerStub(crdtA));
        final Timestamp txn2GlobalTimestamp = globalTimestampGen.generateNew();
        mockManager.commitTxn(txn2Matcher());
        EasyMock.expectLastCall().andDelegateTo(new CommitingManagerStub(null, false));
        replayAll();

        // Actual test.
        // Commit some operations on object A in txn1.
        txn1 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.EMPTY_LIST,
                localTimestampGen.generateNew());
        final IntegerTxnLocal crdtAViewTxn1 = txn1.get(idCrdtA, true, IntegerVersioned.class);
        crdtAViewTxn1.add(1);
        txn1.commitAsync(null);
        assertEquals(TxnStatus.COMMITTED_LOCAL, txn1.getStatus());
        assertEquals(txn1GlobalTimestampProposal, txn1.getGlobalTimestamp());

        // Leave txn1 only locally committed, execute locally timestamped
        // operations on A.
        crdtA.execute((CRDTObjectOperationsGroup<IntegerVersioned>) txn1.getObjectLocalOperations(idCrdtA),
                CRDTOperationDependencyPolicy.CHECK);

        // Start txn2 depending on txn1.
        txn2 = new TxnHandleImpl(mockManager, globalCausalityClock, Collections.singletonList(txn1),
                localTimestampGen.generateNew());
        // Mess up even further - change txn1 global timestamp now, it should
        // not affect anything.
        txn1.setGlobalTimestamp(txn1GlobalTimestampFinal);
        assertEquals(Collections.singletonList(txn1), txn2.getLocalVisibleTransactions());
        final IntegerTxnLocal crdtAViewTxn2 = txn2.get(idCrdtA, true, IntegerVersioned.class);
        assertEquals(new Integer(1), crdtAViewTxn2.getValue());
        crdtAViewTxn2.add(2);
        txn2.commitAsync(null);
        assertEquals(TxnStatus.COMMITTED_LOCAL, txn2.getStatus());
        assertEquals(null, txn2.getGlobalTimestamp());

        // Operations generated by txn2 should depend on txn1's operations.
        final CRDTObjectOperationsGroup<?> localOpsA = txn2.getObjectLocalOperations(idCrdtA);
        assertEquals(txn2.getLocalTimestamp(), localOpsA.getBaseTimestamp());
        assertTrue(localOpsA.getDependency().includes(txn1.getLocalTimestamp()));

        // Commit txn1 globally, then txn2.
        txn1.markGloballyCommitted();
        txn2.markFirstLocalVisibleTransactionGlobal();
        txn2.setGlobalTimestamp(txn2GlobalTimestamp);
        txn2.markGloballyCommitted();

        // Verify that global commit of txn1 is now reflected in txn2.
        assertTrue(txn2.getLocalVisibleTransactions().isEmpty());
        final Collection<CRDTObjectOperationsGroup<?>> globalOps = txn2.getAllGlobalOperations();
        assertEquals(1, globalOps.size());
        final CRDTObjectOperationsGroup<?> globalOpsA = globalOps.iterator().next();
        assertEquals(txn2.getGlobalTimestamp(), globalOpsA.getBaseTimestamp());
        assertTrue(globalOpsA.getDependency().includes(txn1.getGlobalTimestamp()));
        assertFalse(globalOpsA.getDependency().includes(txn1.getLocalTimestamp()));

        // Verify mock calls.
        verifyAll();
    }

    private TxnHandleImpl txn1Matcher() {
        new AbstractTxnMatcher() {
            @Override
            public boolean matches(Object arg) {
                return txn1.equals(arg);
            }
        };
        return null;
    }

    private TxnHandleImpl txn2Matcher() {
        new AbstractTxnMatcher() {
            @Override
            public boolean matches(Object arg) {
                return txn2.equals(arg);
            }
        };
        return null;
    }

    private class DummyManager implements TxnManager {
        @Override
        public TxnHandleImpl beginTxn(CachePolicy cp, boolean readOnly) {
            return null;
        }

        @Override
        public <V extends CRDT<V>> TxnLocalCRDT<V> getObjectTxnView(TxnHandleImpl txn, CRDTIdentifier id,
                boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
                ConsistentSnapshotVersionNotFoundException {
            return null;
        }

        @Override
        public void discardTxn(TxnHandleImpl txn) {
        }

        @Override
        public void commitTxn(TxnHandleImpl txn) {
        }
    }

    private class CommitingManagerStub extends DummyManager {
        private final Timestamp globalTimestamp;
        private final boolean commitGlobal;

        public CommitingManagerStub(Timestamp globalTimestamp, boolean commitGlobal) {
            this.globalTimestamp = globalTimestamp;
            this.commitGlobal = commitGlobal;
        }

        @Override
        public void commitTxn(TxnHandleImpl txn) {
            txn.markLocallyCommitted();
            if (globalTimestamp != null) {
                txn.setGlobalTimestamp(globalTimestamp);
            }
            if (commitGlobal) {
                txn.markGloballyCommitted();
            }
        }
    }

    private class ObjectProviderManagerStub extends DummyManager {
        private final BaseCRDT<IntegerVersioned> crdt;

        public ObjectProviderManagerStub(BaseCRDT<IntegerVersioned> crdt) {
            this.crdt = crdt;
        }

        @Override
        public <V extends CRDT<V>> TxnLocalCRDT<V> getObjectTxnView(TxnHandleImpl txn, CRDTIdentifier id,
                boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
                ConsistentSnapshotVersionNotFoundException {
            return (TxnLocalCRDT<V>) crdt.getTxnLocalCopy(txn.getAllVisibleTransactionsClock(), txn);
        }
    }

    private abstract class AbstractTxnMatcher implements IArgumentMatcher {
        public AbstractTxnMatcher() {
            EasyMock.reportMatcher(this);
        }

        @Override
        public abstract boolean matches(Object arg);

        @Override
        public void appendTo(StringBuffer buffer) {
            buffer.append("matches txn1");
        }
    }
}
