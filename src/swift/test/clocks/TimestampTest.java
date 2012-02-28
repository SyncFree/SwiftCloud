package swift.test.clocks;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;

public class TimestampTest {

    @Test
    public void cloneTest() {
        IncrementalTimestampGenerator gen = new IncrementalTimestampGenerator( "s1");
        Timestamp t1 = gen.generateNew();
        Timestamp t2 = t1.clone();
 
        assertTrue(t1.equals(t2));
        assertTrue(t1.includes(t2));
    }

    @Test
    public void diffIdsTest() {
        IncrementalTimestampGenerator gen1 = new IncrementalTimestampGenerator( "s1");
        Timestamp t1 = gen1.generateNew();
        IncrementalTimestampGenerator gen2 = new IncrementalTimestampGenerator( "s2");
        Timestamp t2 = gen2.generateNew();
 
        assertTrue(! t1.equals(t2));
        assertTrue(! t1.includes(t2));
    }

}
