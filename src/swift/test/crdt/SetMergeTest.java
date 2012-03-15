package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SetIntegers;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

public class SetMergeTest {
    TxnHandle txn1, txn2;
    SetIntegers i1, i2;

    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
        txn1 = new TxnHandleForTesting("client1", ClockFactory.newClock());
        i1 = txn1.get(new CRDTIdentifier("A", "Int"), true, SetIntegers.class);

        txn2 = new TxnHandleForTesting("client2", ClockFactory.newClock());
        i2 = txn2.get(new CRDTIdentifier("A", "Int"), true, SetIntegers.class);
    }

    // Merge with empty set
    @Test
    public void mergeEmpty1() {
        i1.insert(5);
        i1.merge(i2);
        assertTrue(i1.lookup(5));
    }

    // Merge with empty set
    @Test
    public void mergeEmpty2() {
        i2.insert(5);
        i1.merge(i2);
        assertTrue(i1.lookup(5));
    }

    @Test
    public void mergeNonEmpty() {
        i1.insert(5);
        i2.insert(6);
        i1.merge(i2);
        assertTrue(i1.lookup(5));
        assertTrue(i1.lookup(6));
    }

    @Test
    public void mergeConcurrentInsert() {
        i1.insert(5);
        i2.insert(5);
        i1.merge(i2);
        assertTrue(i1.lookup(5));
    }

    @Test
    public void mergeConcurrentRemove() {
        i1.remove(5);
        i2.remove(5);
        i1.merge(i2);
        assertTrue(!i1.lookup(5));
        assertTrue(!i1.lookup(5));
    }

    @Test
    public void mergeConcurrentAddRem() {
        i1.insert(5);
        i2.remove(5);
        i1.merge(i2);
        assertTrue(i1.lookup(5));

        i1.remove(5);
        assertTrue(!i1.lookup(5));
    }

    @Test
    public void mergeConcurrentCausal() {
        i1.insert(5);
        i1.remove(5);
        i2.insert(5);
        i1.merge(i2);
        assertTrue(i1.lookup(5));
    }

    @Test
    public void mergeConcurrentCausal2() {
        i1.insert(5);
        i1.remove(5);
        i2.insert(5);
        i2.remove(5);
        i1.merge(i2);
        assertTrue(!i1.lookup(5));

        i2.remove(5);
        i1.merge(i2);
        assertTrue(!i1.lookup(5));

    }
}
