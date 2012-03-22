package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class IntegerMergeTest {
    TxnHandle txn1, txn2;
    IntegerTxnLocal i1, i2;

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        txn1 = new TxnHandleForTestingLocalBehaviour("client1", ClockFactory.newClock());
        i1 = txn1.get(new CRDTIdentifier("A", "Int"), true, IntegerVersioned.class);

        txn2 = new TxnHandleForTestingLocalBehaviour("client2", ClockFactory.newClock());
        i2 = txn2.get(new CRDTIdentifier("A", "Int"), true, IntegerVersioned.class);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        i1.add(5);
        i1.merge(i2);
        assertTrue(i1.value() == 5);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        i2.add(5);
        i1.merge(i2);
        assertTrue(i1.value() == 5);
    }

    @Test
    public void mergeNonEmpty() {
        i1.add(5);
        i2.add(6);
        i1.merge(i2);
        assertTrue(i1.value() == 11);
        assertTrue(i2.value() == 6);
    }

    @Test
    public void mergeConcurrentRemove() {
        i1.sub(5);
        i2.sub(5);
        i1.merge(i2);
        assertTrue(i1.value() == -10);
    }

    @Test
    public void mergeConcurrentAddRem() {
        i1.add(5);
        i2.sub(5);
        i1.merge(i2);
        assertTrue(i1.value() == 0);

        i1.sub(5);
        assertTrue(i1.value() == -5);
    }

    @Test
    public void mergeConcurrentCausal() {
        i1.add(5);
        i1.sub(5);
        i2.add(5);
        i1.merge(i2);
        assertTrue(i1.value() == 5);
    }

    @Test
    public void mergeConcurrentCausal2() {
        i1.add(5);
        i1.sub(5);
        i2.add(5);
        i2.sub(5);
        i1.merge(i2);
        assertTrue(i1.value() == 0);

        i2.sub(5);
        i1.merge(i2);
        assertTrue(i1.value() == -5);

    }
}
