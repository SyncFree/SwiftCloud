/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt.logoot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.TripleTimestamp;
import swift.crdt.TesterUtils;

/**
 *
 * @author urso
 */
// FIXME: Adapt this tests similarly to IntegerMergeTest and others.
public class LogootVersionedTest {
    
    static final LogootIdentifier a = new LogootIdentifier(1), 
            b = new LogootIdentifier(1), c = new LogootIdentifier(2), 
            d = new LogootIdentifier(1), e = new LogootIdentifier(1);
    static final Set<TripleTimestamp> t = new HashSet<TripleTimestamp>(),             
            u = new HashSet<TripleTimestamp>(), v = new HashSet<TripleTimestamp>();
    static final CausalityClock finalClock = ClockFactory.newClock();

    public LogootVersionedTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        a.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("x", 1, 2)));
        b.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("x", 2, 3)));
        c.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("x", 2, 3)));
        c.addComponent(new Component(42, TesterUtils.generateTripleTimestamp("y", 1, 5)));
        d.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("y", 1, 3)));
        e.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("y", 2, 3)));
        
        t.add(TesterUtils.generateTripleTimestamp("x", 2, 4));
        t.add(TesterUtils.generateTripleTimestamp("y", 4, 8));
        u.add(TesterUtils.generateTripleTimestamp("x", 2, 4));
        v.add(TesterUtils.generateTripleTimestamp("y", 4, 9));

        finalClock.record(TesterUtils.generateTripleTimestamp("x", 1, 1).getClientTimestamp());
        finalClock.record(TesterUtils.generateTripleTimestamp("x", 2, 1).getClientTimestamp());
        finalClock.record(TesterUtils.generateTripleTimestamp("y", 1, 1).getClientTimestamp());
        finalClock.record(TesterUtils.generateTripleTimestamp("y", 2, 1).getClientTimestamp());
        finalClock.record(TesterUtils.generateTripleTimestamp("y", 4, 1).getClientTimestamp());
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
        x.add(1, e, "eee", new HashSet(t));
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", new HashSet(u));
        x.add(1, b, "bbb", new HashSet(v));
        x.add(1, a, "aaa", null);
        LogootVersioned lv = new LogootVersioned();
        lv.setDoc(x);
        
        CausalityClock clock = ClockFactory.newClock();
        clock.record(TesterUtils.generateTripleTimestamp("x", 1, 1).getClientTimestamp());
        assertEquals("aaa\n", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("y", 1, 1).getClientTimestamp());
        assertEquals("aaa\nccc\nddd\n", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("y", 2, 1).getClientTimestamp());
        assertEquals("aaa\nccc\nddd\neee\n", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("x", 2, 1).getClientTimestamp());
        assertEquals("aaa\nbbb\nddd\n", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("y", 4, 1).getClientTimestamp());
        assertEquals("aaa\nddd\n", lv.getValue(clock).toString());
    }

    @Test
    @Ignore
    public void testPrune() {
        LogootDocumentWithTombstones<String> x = new LogootDocumentWithTombstones();
        x.add(1, e, "eee", new HashSet(t));
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", new HashSet(u));
        x.add(1, b, "bbb", new HashSet(v));
        x.add(1, a, "aaa", null);
        LogootVersioned lv = new LogootVersioned();
        lv.setDoc(x);

        assertEquals(7, lv.getDoc().size());
        assertEquals(a, lv.getDoc().idTable.get(1));
        assertEquals("aaa\nddd\n", lv.getValue(finalClock).toString());
        
        CausalityClock clock = ClockFactory.newClock();
        clock.record(TesterUtils.generateTripleTimestamp("x", 1, 1).getClientTimestamp());
        clock.record(TesterUtils.generateTripleTimestamp("y", 1, 1).getClientTimestamp());        
        lv.pruneImpl(clock);
        
        assertEquals(7, lv.getDoc().size());
        assertEquals("aaa\nddd\n", lv.getValue(finalClock).toString());
        
        clock.record(TesterUtils.generateTripleTimestamp("x", 2, 1).getClientTimestamp());
        lv.pruneImpl(clock);
        
        assertEquals(5, lv.getDoc().size());
        assertFalse("c not pruned", lv.getDoc().idTable.contains(c));
        assertFalse("e not pruned", lv.getDoc().idTable.contains(e));
        assertEquals("aaa\nddd\n", lv.getValue(finalClock).toString());

        clock.record(TesterUtils.generateTripleTimestamp("y", 2, 1).getClientTimestamp());
        clock.record(TesterUtils.generateTripleTimestamp("y", 4, 1).getClientTimestamp());

        lv.pruneImpl(clock);
        assertEquals(4, lv.getDoc().size());
        assertEquals("aaa\nddd\n", lv.getValue(finalClock).toString());
    }
    
    
    @Test
    @Ignore
    public void testMerge() {
        // TODO review the generated test code and remove the default call to fail.
        LogootDocumentWithTombstones<String> x = new LogootDocumentWithTombstones(),
                y = new LogootDocumentWithTombstones();
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", null);
        x.add(1, a, "aaa", null);
        
        y.add(1, e, "eee", null);
        y.add(1, c, "ccc", null);
        y.add(1, b, "bbb", null);
        
        assertEquals("aaa\nccc\nddd\n", x.toString());
        assertEquals("bbb\nccc\neee\n", y.toString());
        assertFalse(x.equals(y));
        
        x.merge(y);
        assertEquals("aaa\nbbb\nccc\nddd\neee\n", x.toString());
        assertEquals("bbb\nccc\neee\n", y.toString());
        y.merge(x);
        assertEquals("aaa\nbbb\nccc\nddd\neee\n", x.toString());
        assertEquals("aaa\nbbb\nccc\nddd\neee\n", y.toString());
        assertTrue(x.equals(y));
    }
    
    @Test
    @Ignore
    public void testMergeTombstones() {
        // TODO review the generated test code and remove the default call to fail.
        LogootVersioned<String> x = new LogootDocumentWithTombstones(),
                y = new LogootDocumentWithTombstones();
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", new HashSet<TripleTimestamp>(t));
        x.add(1, b, "bbb", null);
        x.add(1, a, "aaa", new HashSet<TripleTimestamp>(u));
        
        y.add(1, e, "eee", null);
        y.add(1, d, "ddd", new HashSet<TripleTimestamp>(u));
        y.add(1, c, "ccc", new HashSet<TripleTimestamp>(v));
        y.add(1, a, "aaa", new HashSet<TripleTimestamp>(v));
        
        assertEquals("bbb\nddd\n", x.toString());
        assertEquals("eee\n", y.toString());
        
        x.merge(y);
        assertFalse(x.equals(y));
        assertEquals("bbb\neee\n", x.toString());
        assertEquals("eee\n", y.toString());
        assertEquals(7, x.document.size());
        assertEquals(2, x.tombstones.get(1).size());
        assertNull(x.tombstones.get(2));
        assertEquals(2, x.tombstones.get(3).size());
        assertEquals(1, x.tombstones.get(4).size());
        
        y.merge(x);
        assertEquals("bbb\neee\n", y.toString());
        assertTrue(x.equals(y));
    }
}
