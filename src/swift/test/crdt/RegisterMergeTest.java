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

public class RegisterMergeTest {
    IntegerVersioned i1, i2;
    TxnHandleForTestingLocalBehaviour txn1, txn2;

    private IntegerTxnLocal getTxnLocal(IntegerVersioned i, TxnHandleForTestingLocalBehaviour txn) {
        return (IntegerTxnLocal) i.getTxnLocalCopy(i.getClock(), txn);
    }

    private void printInformtion(IntegerVersioned i, TxnHandleForTestingLocalBehaviour txn) {
        System.out.println(i.getClock());
        System.out.println(txn.getClock());
        System.out.println(getTxnLocal(i, txn).value());
    }

    private void merge() {
        i1.merge(i2);
        txn1.updateClock(txn2.getClock());
    }

    private void registerUpdate(int value, IntegerVersioned i, TxnHandleForTestingLocalBehaviour txn) {
        txn1.registerOperation(i, new IntegerUpdate(txn.nextTimestamp(), value));
    }

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        i1 = new IntegerVersioned();
        i1.setClock(ClockFactory.newClock());
        i1.setPruneClock(ClockFactory.newClock());

        i2 = new IntegerVersioned();
        i2.setClock(ClockFactory.newClock());
        i2.setPruneClock(ClockFactory.newClock());
        txn1 = new TxnHandleForTestingLocalBehaviour("client1", ClockFactory.newClock());
        txn2 = new TxnHandleForTestingLocalBehaviour("client2", ClockFactory.newClock());
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        i1.executeOperation(new IntegerUpdate(txn1.nextTimestamp(), 5));
        printInformtion(i1, txn1);
        i1.merge(i2);
        printInformtion(i1, txn1);

        assertTrue(getTxnLocal(i1, txn1).value() == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        registerUpdate(5, i2, txn2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, txn1).value() == 5);
    }

    @Test
    public void mergeNonEmpty() {
        registerUpdate(5, i1, txn1);
        registerUpdate(6, i2, txn2);
        i1.merge(i2);
        assertTrue(getTxnLocal(i1, txn1).value() == 11);
    }

    @Test
    public void mergeConcurrentAddRem() {
        registerUpdate(5, i1, txn1);
        registerUpdate(-5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).value() == 0);

        registerUpdate(-5, i1, txn1);
        assertTrue(getTxnLocal(i1, txn1).value() == -5);
    }

    @Test
    public void mergeConcurrentCausal() {
        registerUpdate(5, i1, txn1);
        registerUpdate(-5, i1, txn1);
        registerUpdate(5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).value() == 5);
    }

    @Test
    public void mergeConcurrentCausal2() {
        registerUpdate(5, i1, txn1);
        registerUpdate(-5, i1, txn1);
        registerUpdate(5, i2, txn2);
        registerUpdate(-5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).value() == 0);

        registerUpdate(-5, i2, txn2);
        merge();
        assertTrue(getTxnLocal(i1, txn1).value() == -5);
    }

    // TODO Tests for prune

}
