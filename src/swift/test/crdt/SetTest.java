package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SetVersioned;
import swift.crdt.interfaces.TxnHandle;

public class SetTest {
    TxnHandle txn;
    SetVersioned<Integer> i;

    @Before
    public void setUp() {
        txn = new TxnHandleForTesting("client1", ClockFactory.newClock());
        i = txn.get(new CRDTIdentifier("A", "Int"), true, SetVersioned.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void mergeTest() {
        TxnHandle txn1 = new TxnHandleForTesting("client1", ClockFactory.newClock());
        SetVersioned<Integer> i1 = txn1.get(new CRDTIdentifier("A", "Int"), true, SetVersioned.class);
        i1.insert(5);

        TxnHandle txn2 = new TxnHandleForTesting("client2", ClockFactory.newClock());
        SetVersioned<Integer> i2 = txn2.get(new CRDTIdentifier("A", "Int"), true, SetVersioned.class);

        i2.insert(10);
        i1.merge(i2);

    }

}
