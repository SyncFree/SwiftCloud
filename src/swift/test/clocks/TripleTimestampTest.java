package swift.test.clocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.exceptions.InvalidParameterException;

public class TripleTimestampTest {

    @Test
    public void simpleTest() throws InvalidParameterException {
        IncrementalTimestampGenerator gen1 = new IncrementalTimestampGenerator( "s1");
        Timestamp t1 = gen1.generateNew();

        IncrementalTripleTimestampGenerator gen2 = new IncrementalTripleTimestampGenerator( t1); 
        Timestamp t11 = gen2.generateNew();
        Timestamp t12 = gen2.generateNew();
 
        assertTrue(t1.includes(t1));
        assertTrue(t1.includes(t11));
        assertTrue(t1.includes(t12));
        assertTrue(!t11.includes(t1));
        assertTrue(t11.includes(t11));
        assertTrue(!t11.includes(t12));
        assertTrue(!t12.includes(t1));
        assertTrue(!t12.includes(t11));
        assertTrue(t12.includes(t12));

        assertTrue(t1.equals(t1));
        assertTrue(!t1.equals(t11));
        assertTrue(!t1.equals(t12));
        assertTrue(!t11.equals(t1));
        assertTrue(t11.equals(t11));
        assertTrue(!t11.equals(t12));
        assertTrue(!t12.equals(t1));
        assertTrue(!t12.equals(t11));
        assertTrue(t12.equals(t12));
    }

    @Test
    public void dualTripleGenTest() throws InvalidParameterException {
        IncrementalTimestampGenerator gen1 = new IncrementalTimestampGenerator( "s1");
        Timestamp t1 = gen1.generateNew();
        Timestamp t2 = gen1.generateNew();

        IncrementalTripleTimestampGenerator gen2 = new IncrementalTripleTimestampGenerator( t1); 
        Timestamp t11 = gen2.generateNew();
        IncrementalTripleTimestampGenerator gen3 = new IncrementalTripleTimestampGenerator( t2); 
        Timestamp t21 = gen3.generateNew();
 
        assertTrue(t1.includes(t1));
        assertTrue(!t1.includes(t2));
        assertTrue(t1.includes(t11));
        assertTrue(!t1.includes(t21));

        assertTrue(!t2.includes(t1));
        assertTrue(t2.includes(t2));
        assertTrue(!t2.includes(t11));
        assertTrue(t2.includes(t21));

        assertTrue(!t11.includes(t1));
        assertTrue(!t11.includes(t2));
        assertTrue(t11.includes(t11));
        assertTrue(!t11.includes(t21));

        assertTrue(!t21.includes(t1));
        assertTrue(!t21.includes(t2));
        assertTrue(!t21.includes(t11));
        assertTrue(t21.includes(t21));
        
        assertTrue(t1.equals(t1));
        assertTrue(!t1.equals(t2));
        assertTrue(!t1.equals(t11));
        assertTrue(!t1.equals(t21));

        assertTrue(!t2.equals(t1));
        assertTrue(t2.equals(t2));
        assertTrue(!t2.equals(t11));
        assertTrue(!t2.equals(t21));

        assertTrue(!t11.equals(t1));
        assertTrue(!t11.equals(t2));
        assertTrue(t11.equals(t11));
        assertTrue(!t11.equals(t21));

        assertTrue(!t21.equals(t1));
        assertTrue(!t21.equals(t2));
        assertTrue(!t21.equals(t11));
        assertTrue(t21.equals(t21));
    }

    @Test
    public void testWithBaseTimestamp() throws InvalidParameterException {
        final Timestamp origTimestamp = new IncrementalTimestampGenerator("client").generateNew();
        final IncrementalTripleTimestampGenerator gen = new IncrementalTripleTimestampGenerator(origTimestamp);
        final TripleTimestamp tt = gen.generateNew();

        final Timestamp newTimestamp = new IncrementalTimestampGenerator("server").generateNew();
        final TripleTimestamp ttWithNew = tt.withBaseTimestamp(newTimestamp);

        assertEquals(tt.getSecondaryCounter(), ttWithNew.getSecondaryCounter());
        assertEquals(newTimestamp.getIdentifier(), ttWithNew.getIdentifier());
        assertEquals(newTimestamp.getCounter(), ttWithNew.getCounter());
        // Old instance is unaffected.
        assertEquals(origTimestamp.getIdentifier(), tt.getIdentifier());
        assertEquals(origTimestamp.getCounter(), tt.getCounter());
    }
}
