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
    TxnHandleForTestingLocalBehaviour txn1, txn2;

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
        i1.merge(i2);

        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        i2.executeOperation(new IntegerUpdate(txn2.nextTimestamp(), 5));
        i1.merge(i2);
        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == 5);
    }

    @Test
    public void mergeNonEmpty() {
        i1.executeOperation(new IntegerUpdate(txn1.nextTimestamp(), 5));
        i2.executeOperation(new IntegerUpdate(txn2.nextTimestamp(), 6));
        i1.merge(i2);
        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == 11);
    }

    @Test
    public void mergeConcurrentAddRem() {
        txn1.registerOperation(i1, new IntegerUpdate(txn1.nextTimestamp(), 5));
        txn2.registerOperation(i2, new IntegerUpdate(txn2.nextTimestamp(), -5));
        i1.merge(i2);
        txn1.updateClock(txn2.getClock());

        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == 0);

        txn1.registerOperation(i1, new IntegerUpdate(txn1.nextTimestamp(), -5));
        System.out.println(i1.getClock());
        System.out.println(txn1.getClock());
        System.out.println(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value());
        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == -5);
    }

    @Test
    public void mergeConcurrentCausal() {
        txn1.registerOperation(i1, new IntegerUpdate(txn1.nextTimestamp(), 5));
        txn1.registerOperation(i1, new IntegerUpdate(txn1.nextTimestamp(), -5));
        txn2.registerOperation(i2, new IntegerUpdate(txn1.nextTimestamp(), 5));

        i1.merge(i2);
        txn1.updateClock(txn2.getClock());

        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == 5);
    }

    @Test
    public void mergeConcurrentCausal2() {
        txn1.registerOperation(i1, new IntegerUpdate(txn1.nextTimestamp(), 5));
        txn1.registerOperation(i1, new IntegerUpdate(txn1.nextTimestamp(), -5));
        txn2.registerOperation(i2, new IntegerUpdate(txn1.nextTimestamp(), 5));
        txn2.registerOperation(i2, new IntegerUpdate(txn1.nextTimestamp(), -5));
        i1.merge(i2);
        txn1.updateClock(txn2.getClock());

        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == 0);

        txn2.registerOperation(i2, new IntegerUpdate(txn1.nextTimestamp(), -5));
        i1.merge(i2);
        txn1.updateClock(txn2.getClock());

        assertTrue(((IntegerTxnLocal) i1.getTxnLocalCopy(i1.getClock(), txn1)).value() == -5);
    }

    // TODO Tests for prune
}
