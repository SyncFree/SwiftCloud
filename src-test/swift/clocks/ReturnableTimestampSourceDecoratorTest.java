package swift.clocks;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ReturnableTimestampSourceDecoratorTest {

    private ReturnableTimestampSourceDecorator<Timestamp> generator;

    @Before
    public void setUp() {
        generator = new ReturnableTimestampSourceDecorator<Timestamp>(new IncrementalTimestampGenerator("abc"));
    }

    @Test
    public void test() {
        final Timestamp ts1 = generator.generateNew();
        final Timestamp ts2 = generator.generateNew();
        assertFalse(ts2.equals(ts1));
        generator.returnLastTimestamp();
        assertEquals(ts2, generator.generateNew());
        final Timestamp ts3 = generator.generateNew();
        assertFalse(ts2.equals(ts3));
        generator.returnLastTimestamp();
        assertEquals(ts3, generator.generateNew());
        generator.returnLastTimestamp();
        assertEquals(ts3, generator.generateNew());
    }

}
