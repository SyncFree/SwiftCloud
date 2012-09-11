package swift.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class DirectoryTest {
    TxnHandle txn;
    DirectoryTxnLocal dir;

    @Before
    public void setUp() throws SwiftException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        dir = txn.get(new CRDTIdentifier("A", "Dir"), true, DirectoryVersioned.class);
    }

    @Test
    public void initTest() {
        assertTrue(dir.getValue().isEmpty());
    }

    @Test
    public void emptyTest() {
        // lookup on empty set
        assertTrue(!dir.contains("x", IntegerVersioned.class));
        assertTrue(dir.getValue().isEmpty());
    }

    @Test
    public void insertTest() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        // create one element

        CRDTIdentifier id1 = dir.putNoReturn("x", IntegerVersioned.class);
        IntegerTxnLocal i = txn.get(id1, true, IntegerVersioned.class);

        System.out.println(dir.getValue());
        IntegerTxnLocal i2 = dir.get("x", IntegerVersioned.class);
        assertTrue(i2.getValue() == 0);

        assertTrue(dir.contains("x", IntegerVersioned.class));
    }

    /*
     * @Test public void deleteTest() { int v = 5; int w = 7; i.insert(v);
     * i.insert(w);
     * 
     * i.remove(v); assertTrue(!i.lookup(v)); assertTrue(i.lookup(w));
     * 
     * // remove should be idempotent i.remove(v); assertTrue(!i.lookup(v));
     * assertTrue(i.lookup(w));
     * 
     * i.remove(w); assertTrue(!i.lookup(v)); assertTrue(!i.lookup(w)); }
     */
}
