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
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.TripleTimestamp;
import swift.crdt.TesterUtils;

/**
 *
 * @author urso
 */
public class LogootDocumentWithTombstonesTest {
    
    static final LogootIdentifier a = new LogootIdentifier(1), 
            b = new LogootIdentifier(1), c = new LogootIdentifier(2), 
            d = new LogootIdentifier(1), e = new LogootIdentifier(1);
    static final Set<TripleTimestamp> t = new HashSet<TripleTimestamp>(),             
            u = new HashSet<TripleTimestamp>(), v = new HashSet<TripleTimestamp>();
    
    public LogootDocumentWithTombstonesTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        new IncrementalTimestampGenerator("x");
        a.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("x", 1, 2)));
        b.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("x", 1, 3)));
        c.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("x", 1, 3)));
        c.addComponent(new Component(42, TesterUtils.generateTripleTimestamp("y", 1, 5)));
        d.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("y", 1, 3)));
        e.addComponent(new Component(5, TesterUtils.generateTripleTimestamp("y", 2, 3)));
        
        t.add(TesterUtils.generateTripleTimestamp("y", 4, 9));
        t.add(TesterUtils.generateTripleTimestamp("y", 4, 8));
        u.add(TesterUtils.generateTripleTimestamp("x", 7, 7));
        v.add(TesterUtils.generateTripleTimestamp("y", 4, 9));
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
    public void testInsert() {
        // TODO review the generated test code and remove the default call to fail.
        LogootDocumentWithTombstones x = new LogootDocumentWithTombstones();
        x.insert(a, "aaa");
        x.insert(e, "eee");
        x.insert(d, "ddd");
        x.insert(c, "ccc");
        x.insert(b, "bbb");
        
        assertEquals("aaa\nbbb\nccc\nddd\neee\n", x.toString());
    }

    @Test
    public void testDelete() {
        // TODO review the generated test code and remove the default call to fail.
        LogootDocumentWithTombstones<String> x = new LogootDocumentWithTombstones();
        x.add(1, e, "eee", null);
        x.add(1, d, "ddd", null);
        x.add(1, c, "ccc", null);
        x.add(1, b, "bbb", null);
        x.add(1, a, "aaa", null);
        assertEquals("aaa\nbbb\nccc\nddd\neee\n", x.toString());

        x.delete(a, TesterUtils.generateTripleTimestamp("y", 5, 6));
        x.delete(d, TesterUtils.generateTripleTimestamp("y", 5, 9));
        x.delete(c, TesterUtils.generateTripleTimestamp("y", 5, 8));
        x.delete(a, TesterUtils.generateTripleTimestamp("y", 5, 7));

        assertEquals("bbb\neee\n", x.toString());
        assertEquals(7, x.document.size());
        assertEquals(2, x.tombstones.get(1).size());
    } 
}
