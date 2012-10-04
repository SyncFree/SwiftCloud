package swift.clocks;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author mzawirski
 */
public class TripleTimestampTest {
    @Test
    public void testCompareEquals() {
        final IncrementalTripleTimestampGenerator gen = new IncrementalTripleTimestampGenerator(new TimestampMapping(
                new Timestamp("a", 1)));
        final TripleTimestamp a = gen.generateNew();
        final TripleTimestamp b = gen.generateNew();

        assertFalse(a.equals(b));
        assertFalse(a.compareTo(b) == 0);
        assertTrue(a.equals(a));
        assertTrue(a.compareTo(a) == 0);
    }

    @Test
    public void testIndependenceFromMappings() {
        final IncrementalTripleTimestampGenerator gen = new IncrementalTripleTimestampGenerator(new TimestampMapping(
                new Timestamp("a", 1)));
        final TripleTimestamp a = gen.generateNew();
        final TripleTimestamp aCopy = a.copyWithMappings(a.getMapping().copy());

        assertNotSame(a.getMapping(), aCopy.getMapping());
        assertEquals(a, aCopy);

        a.addSystemTimestamp(new Timestamp("b", 2));
        assertEquals(a, aCopy);
    }
}
