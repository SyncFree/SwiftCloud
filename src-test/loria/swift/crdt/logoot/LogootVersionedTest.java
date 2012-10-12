/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.swift.crdt.logoot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
        lv.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        lv.setDoc(x);
        
        CausalityClock clock = ClockFactory.newClock();
        clock.record(TesterUtils.generateTripleTimestamp("x", 1, 1).getClientTimestamp());
        assertEquals("aaa", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("y", 1, 1).getClientTimestamp());
        assertEquals("aaa\nccc\nddd", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("y", 2, 1).getClientTimestamp());
        assertEquals("aaa\nccc\nddd\neee", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("x", 2, 1).getClientTimestamp());
        assertEquals("aaa\nbbb\nddd", lv.getValue(clock).toString());
        clock.record(TesterUtils.generateTripleTimestamp("y", 4, 1).getClientTimestamp());
        assertEquals("aaa\nddd", lv.getValue(clock).toString());
    }

    @Test
    public void testPrune() {
        LogootVersioned lv = new LogootVersioned();
        lv.init(null, ClockFactory.newClock(), ClockFactory.newClock(), true);
        lv.applyInsert(a, "aaa");
        lv.applyInsert(b, "bbb");
        lv.applyInsert(c, "ccc");
        lv.applyInsert(d, "ddd");
        lv.applyInsert(e, "eee");
        lv.applyDelete(e, TesterUtils.generateTripleTimestamp("x", 2, 4));
        lv.applyDelete(e, TesterUtils.generateTripleTimestamp("y", 4, 8));
        lv.applyDelete(c, TesterUtils.generateTripleTimestamp("x", 2, 5));
        lv.applyDelete(b, TesterUtils.generateTripleTimestamp("y", 4, 9));

        assertEquals(7, lv.getDoc().size());
        assertEquals(a, lv.getDoc().idTable.get(1));
        assertEquals("aaa\nddd", lv.getValue(finalClock).toString());
        
        CausalityClock clock = ClockFactory.newClock();
        clock.record(TesterUtils.generateTripleTimestamp("x", 1, 1).getClientTimestamp());
        clock.record(TesterUtils.generateTripleTimestamp("y", 1, 1).getClientTimestamp());
        lv.pruneImpl(clock);
        
        assertEquals(7, lv.getDoc().size());
        assertEquals("aaa\nddd", lv.getValue(finalClock).toString());
        
        clock.record(TesterUtils.generateTripleTimestamp("x", 2, 1).getClientTimestamp());
        lv.pruneImpl(clock);
        
        assertEquals(5, lv.getDoc().size());
        assertFalse("c not pruned", lv.getDoc().idTable.contains(c));
        assertFalse("e not pruned", lv.getDoc().idTable.contains(e));
        assertEquals("aaa\nddd", lv.getValue(finalClock).toString());

        clock.record(TesterUtils.generateTripleTimestamp("y", 2, 1).getClientTimestamp());
        clock.record(TesterUtils.generateTripleTimestamp("y", 4, 1).getClientTimestamp());

        lv.pruneImpl(clock);
        assertEquals(4, lv.getDoc().size());
        assertEquals("aaa\nddd", lv.getValue(finalClock).toString());
    }
}
