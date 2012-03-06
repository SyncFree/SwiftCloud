package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SetIntegers;
import swift.crdt.interfaces.TxnHandle;

public class SetTest {
    TxnHandle txn;
    SetIntegers i;

    @Before
    public void setUp() {
        txn = new TxnHandleForTesting("client1", ClockFactory.newClock());
        // Unchecked casts like the following seem to be unavoidable in Java 1.6
        // unless we apply some rather complex scheme as given in
        // http://gafter.blogspot.com/search?q=super+type+token
        i = txn.get(new CRDTIdentifier("A", "Int"), true, SetIntegers.class);
    }

    @Test
    public void initTest() {
        assertTrue(i.getValue().isEmpty());
    }

    @Test
    public void lookupTest() {
        i.insert(5);
        assertTrue(i.lookup(5));
        assertTrue(!i.lookup(7));
    }

    @Test
    public void mergeTest() {
        TxnHandle txn1 = new TxnHandleForTesting("client1", ClockFactory.newClock());
        SetIntegers i1 = txn1.get(new CRDTIdentifier("A", "Int"), true, SetIntegers.class);
        i1.insert(5);

        TxnHandle txn2 = new TxnHandleForTesting("client2", ClockFactory.newClock());
        SetIntegers i2 = txn2.get(new CRDTIdentifier("A", "Int"), true, SetIntegers.class);

        i2.insert(10);
        i1.merge(i2);

    }

}
