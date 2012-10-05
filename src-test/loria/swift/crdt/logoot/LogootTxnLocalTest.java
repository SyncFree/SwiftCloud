/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt.logoot;

import java.util.Set;
import loria.swift.application.filesystem.mapper.Content;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.TxnTester;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.SwiftException;

/**
 *
 * @author urso
 */
public class LogootTxnLocalTest {
    
    public static class LogootCRDTMock extends LogootVersionned {

        public LogootCRDTMock() {
        }
        
        @Override
        protected TxnLocalCRDT getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
            return new LogootTxnLocal(id, txn, versionClock, this, new LogootDocument());    
        }
    }
    
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
        i.update("Toto\ngo to the \nbeach.\n");
        assertEquals("Toto\ngo to the \nbeach.\n", i.getText());
        LogootIdentifier id1 = i.getDoc().idTable.get(1), 
                id2 = i.getDoc().idTable.get(2), 
                id3 = i.getDoc().idTable.get(3);
        assertTrue("unordered " + id1 + " and  "  + id2,  id1.compareTo(id2) < 0);
        assertTrue("unordered " + id2 + " and  "  + id3,  id2.compareTo(id3) < 0);
    }

    @Test
    public void idTestInsert() {
        i.update("Toto\ngo to the \nbeach.\n");
        LogootIdentifier id1 = i.getDoc().idTable.get(1), 
                id2 = i.getDoc().idTable.get(2); 
        i.update("Toto\ndon't\ngo to the \nbeach.\n");
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
        i.update("Toto\ngo to the \nbeach.\n");
        LogootIdentifier id1 = i.getDoc().idTable.get(1), 
                id3 = i.getDoc().idTable.get(3); 
        i.update("Toto\nbeach.\n");
        LogootIdentifier id1b = i.getDoc().idTable.get(1), 
                id2b = i.getDoc().idTable.get(2);
        assertEquals(id1, id1b);
        assertEquals(id3, id2b);
    }
}
