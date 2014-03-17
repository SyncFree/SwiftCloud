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

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock.CMP_CLOCK;

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

    @Test
    public void testCompareDifferentEntries() {
        VersionVectorWithExceptions clock1 = new VersionVectorWithExceptions();
        clock1.recordAllUntil(new Timestamp("A", 1));
        clock1.recordAllUntil(new Timestamp("B", 2));

        VersionVectorWithExceptions clock2 = new VersionVectorWithExceptions();
        clock2.recordAllUntil(new Timestamp("B", 2));
        clock2.recordAllUntil(new Timestamp("C", 3));

        assertEquals(CMP_CLOCK.CMP_CONCURRENT, clock1.compareTo(clock2));
    }

}
