package swift.test.crdt;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import swift.clocks.ClockFactory;
import swift.crdt.CRDTIdentifier;
import swift.crdt.CRDTIntegerTxn;
import swift.crdt.interfaces.TxnHandle;

public class CRDTIntegerTxnTest {

    @Test
    public void addTest() {
        final int incr = 10;
        TxnHandle txn = new TxnHandleForTesting("client1", ClockFactory.newClock());
        CRDTIntegerTxn i = txn.get(new CRDTIdentifier("A", "Int"), true, CRDTIntegerTxn.class);

        i.add(incr);
        assertTrue(incr == i.value());
    }

}
