package swift.clocks;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

/**
 * @author mzawirski
 */
public class TimestampMappingTest {
    private static final Timestamp CLIENT_TIMESTAMP_A = new Timestamp("a", 1);
    private static final Timestamp CLIENT_TIMESTAMP_B = new Timestamp("b", 1);
    private static final Timestamp SYSTEM_TIMESTAMP_1 = new Timestamp("X", 1);
    private static final Timestamp SYSTEM_TIMESTAMP_2 = new Timestamp("X", 2);
    private static final Timestamp SYSTEM_TIMESTAMP_3 = new Timestamp("Y", 3);
    private TimestampMapping a;
    private TimestampMapping aCopy;
    private TimestampMapping b;

    @Before
    public void setUp() {
        a = new TimestampMapping(CLIENT_TIMESTAMP_A);
        aCopy = a.copy();
        b = new TimestampMapping(CLIENT_TIMESTAMP_B);
    }

    @Test
    public void testInital() {
        assertEquals(CLIENT_TIMESTAMP_A, a.getClientTimestamp());
        assertEquals(Collections.singletonList(CLIENT_TIMESTAMP_A), a.getTimestamps());

        final CausalityClock clock = ClockFactory.newClock();
        assertFalse(a.timestampsIntersect(clock));
        clock.record(CLIENT_TIMESTAMP_A);
        assertTrue(a.timestampsIntersect(clock));

        try {
            a.getSelectedSystemTimestamp();
            fail("Expected no system timestamp");
        } catch (IllegalStateException x) {
            // expected
        }

        try {
            a.getTimestamps().clear();
            fail("Expected non-updatable reference");
        } catch (UnsupportedOperationException x) {
            // expected
        }

        try {
            a.addSystemTimestamps(b);
            fail("Expected being unable to merge different mappings");
        } catch (IllegalArgumentException x) {
            // expected
        }
    }

    @Test
    public void testAddSystemTimestamp() {
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        assertEquals(new HashSet<Timestamp>(Arrays.asList(new Timestamp[] { CLIENT_TIMESTAMP_A, SYSTEM_TIMESTAMP_1 })),
                new HashSet<Timestamp>(a.getTimestamps()));
    }

    @Test
    public void testAddSystemTimestampIdempotent() {
        a.addSystemTimestamp(CLIENT_TIMESTAMP_A);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(CLIENT_TIMESTAMP_A);
        assertEquals(2, a.getTimestamps().size());
    }

    @Test
    public void testAddSystemTimestampsMerge() {
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_2);
        aCopy.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        aCopy.addSystemTimestamp(SYSTEM_TIMESTAMP_3);

        a.addSystemTimestamps(aCopy);
        assertEquals(
                new HashSet<Timestamp>(Arrays.asList(new Timestamp[] { CLIENT_TIMESTAMP_A, SYSTEM_TIMESTAMP_1,
                        SYSTEM_TIMESTAMP_2, SYSTEM_TIMESTAMP_3 })), new HashSet<Timestamp>(a.getTimestamps()));
        assertEquals(
                new HashSet<Timestamp>(Arrays.asList(new Timestamp[] { CLIENT_TIMESTAMP_A, SYSTEM_TIMESTAMP_1,
                        SYSTEM_TIMESTAMP_3 })), new HashSet<Timestamp>(aCopy.getTimestamps()));
    }

    @Test
    public void testClockIntersect() {
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_2);

        CausalityClock clock = ClockFactory.newClock();
        assertFalse(a.timestampsIntersect(clock));
        // subset
        clock.record(SYSTEM_TIMESTAMP_1);
        assertTrue(a.timestampsIntersect(clock));
        // equals
        clock.record(CLIENT_TIMESTAMP_A);
        clock.record(SYSTEM_TIMESTAMP_2);
        assertTrue(a.timestampsIntersect(clock));
        // superset
        clock.record(SYSTEM_TIMESTAMP_3);
        assertTrue(a.timestampsIntersect(clock));

        // intersection
        clock.drop(CLIENT_TIMESTAMP_A.getIdentifier());
        assertTrue(a.timestampsIntersect(clock));
    }

    @Test
    public void testCopy() {
        a.addSystemTimestamp(CLIENT_TIMESTAMP_B);
        assertFalse(a.getTimestamps().size() == aCopy.getTimestamps().size());
    }

    @Test
    public void testSelectedTimestampIsAddOrderIndependent() {
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_2);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_3);

        aCopy.addSystemTimestamp(SYSTEM_TIMESTAMP_3);
        aCopy.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        aCopy.addSystemTimestamp(SYSTEM_TIMESTAMP_2);

        assertEquals(a.getSelectedSystemTimestamp(), aCopy.getSelectedSystemTimestamp());
    }
}
