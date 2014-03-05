/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.clocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        assertFalse(a.anyTimestampIncluded(clock));
        clock.record(CLIENT_TIMESTAMP_A);
        assertTrue(a.anyTimestampIncluded(clock));

        try {
            a.getSelectedSystemTimestamp();
            fail("Expected no system timestamp");
        } catch (IllegalStateException x) {
            // expected
        }

        // try {
        // a.getTimestamps().clear();
        // fail("Expected non-updatable reference");
        // } catch (UnsupportedOperationException x) {
        // // expected
        // }

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
                new HashSet<Timestamp>(Arrays.asList(a.getTimestamps())));
    }

    @Test
    public void testAddSystemTimestampIdempotent() {
        a.addSystemTimestamp(CLIENT_TIMESTAMP_A);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(CLIENT_TIMESTAMP_A);
        assertEquals(2, a.getTimestamps().length);
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
                        SYSTEM_TIMESTAMP_2, SYSTEM_TIMESTAMP_3 })),
                new HashSet<Timestamp>(Arrays.asList(a.getTimestamps())));
        assertEquals(
                new HashSet<Timestamp>(Arrays.asList(new Timestamp[] { CLIENT_TIMESTAMP_A, SYSTEM_TIMESTAMP_1,
                        SYSTEM_TIMESTAMP_3 })), new HashSet<Timestamp>(Arrays.asList(aCopy.getTimestamps())));
    }

    @Test
    public void testClockIntersect() {
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_1);
        a.addSystemTimestamp(SYSTEM_TIMESTAMP_2);

        CausalityClock clock = ClockFactory.newClock();
        // none
        assertFalse(a.anyTimestampIncluded(clock));
        assertFalse(a.allSystemTimestampsIncluded(clock));
        clock.record(SYSTEM_TIMESTAMP_1);
        // subset
        assertTrue(a.anyTimestampIncluded(clock));
        assertFalse(a.allSystemTimestampsIncluded(clock));
        clock.record(CLIENT_TIMESTAMP_A);
        clock.record(SYSTEM_TIMESTAMP_2);
        // equals
        assertTrue(a.anyTimestampIncluded(clock));
        assertTrue(a.allSystemTimestampsIncluded(clock));
        clock.record(SYSTEM_TIMESTAMP_3);
        // superset
        assertTrue(a.anyTimestampIncluded(clock));
        assertTrue(a.allSystemTimestampsIncluded(clock));

        clock.drop(CLIENT_TIMESTAMP_A.getIdentifier());
        // intersection
        assertTrue(a.anyTimestampIncluded(clock));
    }

    @Test
    public void testCopy() {
        a.addSystemTimestamp(CLIENT_TIMESTAMP_B);
        assertFalse(a.getTimestamps().length == aCopy.getTimestamps().length);
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
