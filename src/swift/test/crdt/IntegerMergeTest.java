package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.operations.IntegerUpdate;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class IntegerMergeTest {
    IntegerVersioned i1, i2;
    SwiftTester swift1, swift2;

    private IntegerTxnLocal getTxnLocal(IntegerVersioned i, TxnTester txn) {
        return (IntegerTxnLocal) TesterUtils.getTxnLocal(i, txn);
    }

    private void merge() {
        swift1.merge(i1, i2, swift2);
    }

    private void registerUpdate(int value, IntegerVersioned i, TxnTester txn) {
        txn.registerOperation(i, new IntegerUpdate(txn.nextTimestamp(), value));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        i1 = new IntegerVersioned();
        i1.setClock(ClockFactory.newClock());
        i1.setPruneClock(ClockFactory.newClock());

        i2 = new IntegerVersioned();
        i2.setClock(ClockFactory.newClock());
        i2.setPruneClock(ClockFactory.newClock());
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        registerUpdate(5, i1, swift1.beginTxn());
        i1.merge(i2);

        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerUpdate(5, i2, swift2.beginTxn());
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeNonEmpty() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(6, i2, swift2.beginTxn());
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 11);
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 0);

        registerUpdate(-5, i1, swift1.beginTxn());
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    @Test
    public void mergeConcurrentCausal() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i1, swift1.beginTxn());
        registerUpdate(5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 5);
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerUpdate(5, i1, swift1.beginTxn());
        registerUpdate(-5, i1, swift1.beginTxn());
        registerUpdate(5, i2, swift2.beginTxn());
        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == 0);

        registerUpdate(-5, i2, swift2.beginTxn());
        merge();
        assertTrue(getTxnLocal(i1, swift1.beginTxn()).getValue() == -5);
    }

    // TODO Tests for prune

}
