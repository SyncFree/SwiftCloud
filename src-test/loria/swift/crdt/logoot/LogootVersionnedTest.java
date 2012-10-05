/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt.logoot;

import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.TripleTimestamp;

/**
 *
 * @author urso
 */
public class LogootVersionnedTest {
    
    static final LogootIdentifier a = new LogootIdentifier(1), 
            b = new LogootIdentifier(1), c = new LogootIdentifier(2), 
            d = new LogootIdentifier(1), e = new LogootIdentifier(1);
    static final Set<TripleTimestamp> t = new HashSet<TripleTimestamp>(),             
            u = new HashSet<TripleTimestamp>(), v = new HashSet<TripleTimestamp>();
       
    public LogootVersionnedTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        a.addComponent(new Component(5, new TripleTimestamp("x", 1, 2)));
        b.addComponent(new Component(5, new TripleTimestamp("x", 2, 3)));
        c.addComponent(new Component(5, new TripleTimestamp("x", 2, 3)));
        c.addComponent(new Component(42, new TripleTimestamp("y", 1, 5)));
        d.addComponent(new Component(5, new TripleTimestamp("y", 1, 3)));
        e.addComponent(new Component(5, new TripleTimestamp("y", 2, 3)));
        
        t.add(new TripleTimestamp("x", 2, 4));
        t.add(new TripleTimestamp("y", 4, 8));
        u.add(new TripleTimestamp("x", 2, 4));
        v.add(new TripleTimestamp("y", 4, 9));
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testGetValue() {
        LogootDocumentWithTombstones<String> x = new LogootDocumentWithTombstones();
        x.add(1, e, "eee", t);
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", u);
        x.add(1, b, "bbb", v);
        x.add(1, a, "aaa", null);
        LogootVersionned lv = new LogootVersionned();
        lv.setDoc(x);
        
        CausalityClock clock = ClockFactory.newClock();
        clock.record(new TripleTimestamp("x", 1, 0));
        assertEquals("aaa\n", lv.getValue(clock).toString());
        clock.record(new TripleTimestamp("y", 1, 0));
        assertEquals("aaa\nccc\nddd\n", lv.getValue(clock).toString());
        clock.record(new TripleTimestamp("y", 2, 0));
        assertEquals("aaa\nccc\nddd\neee\n", lv.getValue(clock).toString());
        clock.record(new TripleTimestamp("x", 2, 0));
        assertEquals("aaa\nbbb\nddd\n", lv.getValue(clock).toString());
        clock.record(new TripleTimestamp("y", 4, 0));
        assertEquals("aaa\nddd\n", lv.getValue(clock).toString());
    }
    
    @Test
    public void testRollback() {
        LogootDocumentWithTombstones<String> x = new LogootDocumentWithTombstones();
        x.add(1, e, "eee", t);
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", u);
        x.add(1, b, "bbb", v);
        x.add(1, a, "aaa", null);
        LogootVersionned lv = new LogootVersionned();
        lv.setDoc(x);

        CausalityClock clock = ClockFactory.newClock();
        clock.record(new TripleTimestamp("x", 1, 0));
        clock.record(new TripleTimestamp("x", 2, 0));
        clock.record(new TripleTimestamp("y", 1, 0));
        clock.record(new TripleTimestamp("y", 2, 0));
        clock.record(new TripleTimestamp("y", 4, 0));

        assertEquals(7, lv.getDoc().size());
        assertEquals(a, lv.getDoc().idTable.get(1));
        assertEquals("aaa\nddd\n", lv.getValue(clock).toString());
        
        lv.rollback(new TripleTimestamp("x", 1, 2));
        assertEquals(6, lv.getDoc().size());
        assertEquals(b, lv.getDoc().idTable.get(1));
        assertEquals("ddd\n", lv.getValue(clock).toString());
        
        lv.rollback(new TripleTimestamp("y", 4, 0));        
        assertEquals(6, lv.getDoc().size());
        assertEquals(b, lv.getDoc().idTable.get(1));
        assertEquals("bbb\nddd\n", lv.getValue(clock).toString());
    }
}
