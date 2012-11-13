package swift.clocks;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class VersionVectorWithExceptionsTest {

    private VersionVectorWithExceptions clock;
    private Timestamp tsA1;
    private Timestamp tsA2;
    private Timestamp tsB1;
    private Timestamp tsB2;

    @Before
    public void setUp() {
        clock = new VersionVectorWithExceptions();
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

    @Test
    public void testDropMiddleTimestamp() {
        final int size = 5;
        IncrementalTimestampGenerator siteAGen = new IncrementalTimestampGenerator("a");
        Timestamp tsToDrop = null;
        for (int i = 0; i < size; i++) {
            Timestamp ts = siteAGen.generateNew();
            clock.record(ts);
            if (i == 2) {
                tsToDrop = ts;
            }
        }
        clock.drop(tsToDrop);
        siteAGen = new IncrementalTimestampGenerator("a");
        for (int i = 0; i < size; i++) {
            Timestamp ts = siteAGen.generateNew();
            if (i == 2) {
                assertFalse(clock.includes(ts));
            } else {
                assertTrue(clock.includes(ts));
            }
        }
    }

}
