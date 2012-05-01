package swift.clocks;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class VersionVectorWithExceptionsTest {
    private VersionVectorWithExceptionsOld clock;
    private Timestamp tsA1;
    private Timestamp tsA2;
    private Timestamp tsB1;
    private Timestamp tsB2;

    @Before
    public void setUp() {
        clock = new VersionVectorWithExceptionsOld();
        final IncrementalTimestampGenerator siteAGen = new IncrementalTimestampGenerator("a");
        final IncrementalTimestampGenerator siteBGen = new IncrementalTimestampGenerator("b");
        tsA1 = siteAGen.generateNew();
        tsA2 = siteAGen.generateNew();
        tsB1 = siteBGen.generateNew();
        tsB2 = siteBGen.generateNew();
    }

    @Test
    public void testDropEntriesNoExceptions() {
        // Prepare [2, 2] vector.
        clock.record(tsA1);
        clock.record(tsA2);
        clock.record(tsB1);
        clock.record(tsB2);

        // Drop first entry.
        clock.drop(tsA1.getIdentifier());

        // Should result in [0, 2].
        assertTrue(clock.includes(tsB1));
        assertTrue(clock.includes(tsB2));
        assertFalse(clock.includes(tsA1));
        assertFalse(clock.includes(tsA2));
    }

    @Test
    public void testDropEntriesWithExceptions() {
        // Prepare [2 \ {1}, 2 \ {1}] vector.
        clock.record(tsA2);
        clock.record(tsB2);

        // Drop first entry.
        clock.drop(tsA1.getIdentifier());

        // Should result in [0, 2 \ {1}].
        assertTrue(clock.includes(tsB2));
        assertFalse(clock.includes(tsA2));
    }
    
    @Test
    public void testDropFirstTimestamp() {
        clock.record(tsA1);
        clock.record(tsA2);
        
        clock.drop(tsA1);

        assertFalse(clock.includes(tsA1));
        assertTrue(clock.includes(tsA2));
    }

    @Test
    public void testDropLastTimestamp() {
        clock.record(tsA1);
        clock.record(tsA2);

        clock.drop(tsA2);

        assertTrue(clock.includes(tsA1));
        assertFalse(clock.includes(tsA2));
    }

    @Test
    public void testDropAllTimestamps() {
        clock.record(tsA1);
        clock.record(tsA2);

        clock.drop(tsA2);
        clock.drop(tsA1);

        assertFalse(clock.includes(tsA1));
        assertFalse(clock.includes(tsA2));
    }

    @Test
    public void testDropInexistentTimestamp() {
        clock.record(tsA2);

        clock.drop(tsA1);

        assertFalse(clock.includes(tsA1));
        assertTrue(clock.includes(tsA2));
    }
}
