package swift.client;

import static org.easymock.EasyMock.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.TxnStatus;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * TxnHandleImpl using mock of TxnManager and its much simplified replacement.
 * 
 * @author mzawirski
 */
public class TxnHandleImplTest {

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

    private TxnManager managerMock;
    private CausalityClock globalCausalityClock;
    private List<TxnHandleImpl> localTxns;
    private IncrementalTimestampGenerator localTimestampGen;

    private IntegerVersioned crdtA;
    private CRDTIdentifier idCrdtA;
    private IntegerVersioned crdtB;
    private CRDTIdentifier idCrdtB;

    private TxnHandleImpl txn1;
    private TxnHandleImpl txn2;

    @Before
    public void setUp() {
        managerMock = EasyMock.createStrictMock(TxnManager.class);
        globalCausalityClock = ClockFactory.newClock();
        localTxns = new LinkedList<TxnHandleImpl>();
        localTimestampGen = new IncrementalTimestampGenerator("client");

        crdtA = new IntegerVersioned();
        crdtB = new IntegerVersioned();
        idCrdtA = new CRDTIdentifier("x", "a");
        idCrdtB = new CRDTIdentifier("x", "b");
        crdtA.init(idCrdtA, globalCausalityClock, globalCausalityClock, true);
        crdtB.init(idCrdtA, globalCausalityClock, globalCausalityClock, false);

        txn1 = null;
        txn2 = null;
    }

    private TxnHandleImpl beginTxn() {
        final TxnHandleImpl txn = new TxnHandleImpl(managerMock, globalCausalityClock, localTxns,
                localTimestampGen.generateNew());
        localTxns.add(txn);
        return txn;
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

    @Test
    public void testInit() {
        txn1 = beginTxn();

        assertEquals(TxnStatus.PENDING, txn1.getStatus());
        assertNotNull(txn1.getLocalTimestamp());
        assertNull(txn1.getGlobalTimestamp());

        assertTrue(txn1.getLocalVisibleTransactions().isEmpty());
        final CausalityClock clockAll = txn1.getAllVisibleTransactionsClock();
        final CausalityClock clockGlobal = txn1.getGlobalVisibleTransactionsClock();
        assertNotNull(clockAll);
        assertEquals(CMP_CLOCK.CMP_EQUALS, clockAll.compareTo(clockGlobal));

        assertTrue(txn1.getAllLocalOperations().isEmpty());
        try {
            txn1.getAllGlobalOperations();
            fail();
        } catch (IllegalStateException x) {
            // expected
        }
    }

    @Test
    public void testRollback() {
        // Specify expected TxnManager calls.
        managerMock.discardTxn(txn1Matcher());
        EasyMock.replay(managerMock);

        // Actual test.
        txn1 = beginTxn();
        txn1.rollback();
        assertEquals(TxnStatus.CANCELLED, txn1.getStatus());

        EasyMock.verify(managerMock);
    }

    // FIXME
    @Ignore
    @Test
    public void testSingleTxnReadOnly() throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // Specify expected TxnManager calls.
        EasyMock.expect(managerMock.getObjectTxnView(txn1Matcher(), eq(idCrdtA), eq(true), eq(IntegerVersioned.class)))
                .andReturn(crdtA.getTxnLocalCopy(globalCausalityClock, txn1));
        managerMock.commitTxn(txn1Matcher());
        EasyMock.replay(managerMock);

        // Actual test.
        txn1 = beginTxn();
        final IntegerTxnLocal crdtAView = txn1.get(idCrdtA, true, IntegerVersioned.class);
        assertEquals(new Integer(0), crdtAView.getValue());
        txn1.commit();

        EasyMock.verify(managerMock);
    }
}
