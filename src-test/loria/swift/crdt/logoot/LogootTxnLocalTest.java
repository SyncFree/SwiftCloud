/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt.logoot;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import swift.clocks.ClockFactory;
import swift.crdt.TxnTester;
import swift.exceptions.SwiftException;

/**
 *
 * @author urso
 */
public class LogootTxnLocalTest {    
    TxnTester txn;
    LogootTxnLocal i;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws SwiftException {
        txn = new TxnTester("client1", ClockFactory.newClock());
        i = new LogootTxnLocal(null, txn, txn.getClock(), null, new LogootDocument());
    }

    @Test
    public void initTest() {
        assertEquals("", i.getText());
    }
    
    @Test
    public void updateTest() {
        i.set("Toto\ngo to the \nbeach.\n");
        assertEquals("Toto\ngo to the \nbeach.\n", i.getText());
        LogootIdentifier id1 = i.getDoc().idTable.get(1), 
                id2 = i.getDoc().idTable.get(2), 
                id3 = i.getDoc().idTable.get(3);
        assertTrue("unordered " + id1 + " and  "  + id2,  id1.compareTo(id2) < 0);
        assertTrue("unordered " + id2 + " and  "  + id3,  id2.compareTo(id3) < 0);
    }

    @Test
    public void idTestInsert() {
        i.set("Toto\ngo to the \nbeach.\n");
        LogootIdentifier id1 = i.getDoc().idTable.get(1), 
                id2 = i.getDoc().idTable.get(2); 
        i.set("Toto\ndon't\ngo to the \nbeach.\n");
        LogootIdentifier id1b = i.getDoc().idTable.get(1), 
                id2b = i.getDoc().idTable.get(2),
                id3b = i.getDoc().idTable.get(3);
        assertEquals(id1, id1b);
        assertEquals(id2, id3b);
        assertTrue("unordered " + id1 + " and  "  + id2b,  id1.compareTo(id2b) < 0);
        assertTrue("unordered " + id2b + " and  "  + id2,  id2.compareTo(id2b) > 0);
    }
    
    @Test
    public void idTestDelete() {
        i.set("Toto\ngo to the \nbeach.\n");
        LogootIdentifier id1 = i.getDoc().idTable.get(1), 
                id3 = i.getDoc().idTable.get(3); 
        i.set("Toto\nbeach.\n");
        LogootIdentifier id1b = i.getDoc().idTable.get(1), 
                id2b = i.getDoc().idTable.get(2);
        assertEquals(id1, id1b);
        assertEquals(id3, id2b);
    }
}
