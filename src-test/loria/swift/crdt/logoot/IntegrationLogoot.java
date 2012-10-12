/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt.logoot;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import swift.clocks.ClockFactory;
import swift.crdt.SwiftTester;
import swift.crdt.TesterUtils;
import swift.crdt.TxnTester;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import static org.junit.Assert.*;

/**
 *
 * @author urso
 */
public class IntegrationLogoot {
    SwiftTester swift1, swift2;
    LogootVersioned l1, l2;

    private LogootTxnLocal getTxnLocal(LogootVersioned i, TxnTester txn) {
        return (LogootTxnLocal) TesterUtils.getTxnLocal(i, txn);
    }
    
    @Before
    public void setUp() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        swift1 = new SwiftTester("client1");
        swift2 = new SwiftTester("client2");
        l1 = new LogootVersioned();
        l1.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        l2 = new LogootVersioned();
        l2.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
    } 
    
    
    public IntegrationLogoot() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testInsert() {
        TxnTester txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("aaa\nbbb\n");        
        txn.commit();
        assertEquals("aaa\nbbb\n", getTxnLocal(l1, swift1.beginTxn()).getText()); 
         
    }
    @Test
    public void testInesertEmptyLine(){
        TxnTester txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("aaa\n\nbbb\n\n");        
        txn.commit();
        assertEquals("aaa\n\nbbb\n\n", getTxnLocal(l1, swift1.beginTxn()).getText()); 
    }

    @Test
    public void testDelete() {
        TxnTester txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("aaa\nbbb\n");
        txn.commit();
        
        assertEquals("aaa\nbbb\n", getTxnLocal(l1, swift1.beginTxn()).getText()); 
        LogootIdentifier b = l1.getDoc().idTable.get(2);        
        
        txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("bbb");
        txn.commit();        
        assertEquals("bbb", getTxnLocal(l1, swift1.beginTxn()).getText()); 
        assertEquals(b, l1.getDoc().idTable.get(2));        
    }
    @Test
    public void testDelete2() {
        TxnTester txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("aaa\nbbb");
        txn.commit();
        
        assertEquals("aaa\nbbb", getTxnLocal(l1, swift1.beginTxn()).getText()); 
        LogootIdentifier b = l1.getDoc().idTable.get(2);        
        
        txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("bbb\n");
        txn.commit();        
        assertEquals("bbb\n", getTxnLocal(l1, swift1.beginTxn()).getText()); 
        assertEquals(b, l1.getDoc().idTable.get(2));        
    }
     @Test
    public void testDelete3() {
        TxnTester txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("aaa\nbbb\nccc");
        txn.commit();
        
        assertEquals("aaa\nbbb\nccc", getTxnLocal(l1, swift1.beginTxn()).getText()); 
        LogootIdentifier a = l1.getDoc().idTable.get(1);        
        LogootIdentifier c = l1.getDoc().idTable.get(3);        
        
        txn = swift1.beginTxn(l1);        
        getTxnLocal(l1, txn).set("aaa\nxxx\nccc");
        txn.commit();        
        
        assertEquals("aaa\nxxx\nccc", getTxnLocal(l1, swift1.beginTxn()).getText()); 
         assertEquals(a, l1.getDoc().idTable.get(1));        
         assertEquals(c, l1.getDoc().idTable.get(4));        
    }
}
