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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import swift.clocks.CausalityClock.CMP_CLOCK;

public class VersionVectorWithExceptionsNewTest {
    private VersionVectorWithExceptions clock;
    private VersionVectorWithExceptions clock2;
    private VersionVectorWithExceptions clock3;
    private Timestamp tsA1;
    private Timestamp tsA2;
    private Timestamp tsA3;
    private Timestamp tsA4;
    private Timestamp tsA5;
    private Timestamp tsB1;
    private Timestamp tsB2;
    private Timestamp tsB3;

    @Before
    public void setUp() {
        clock = new VersionVectorWithExceptions();
        clock2 = new VersionVectorWithExceptions();
        clock3 = new VersionVectorWithExceptions();
        final IncrementalTimestampGenerator siteAGen = new IncrementalTimestampGenerator("a");
        final IncrementalTimestampGenerator siteBGen = new IncrementalTimestampGenerator("b");
        tsA1 = siteAGen.generateNew();
        tsA2 = siteAGen.generateNew();
        tsA3 = siteAGen.generateNew();
        tsA4 = siteAGen.generateNew();
        tsA5 = siteAGen.generateNew();
        tsB1 = siteBGen.generateNew();
        tsB2 = siteBGen.generateNew();
        tsB3 = siteBGen.generateNew();
    }

    @Test
    public void testAddOneOtherEntry() {
        // Prepare [5 \ {2,4}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA1);
        clock.record(tsA5);
        clock.record(tsA3);

        assertTrue(clock.includes(tsA1));
        assertFalse(clock.includes(tsA2));
        assertTrue(clock.includes(tsA3));
        assertFalse(clock.includes(tsA4));
        assertTrue(clock.includes(tsA5));
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
    public void testDominates1() {
        // Prepare [5 \ {2,4}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA5);
        clock.record(tsA3);
        // Prepare [5 \ {2,3,4}, 0] vector.
        clock2.record(tsA1);
        clock2.record(tsA5);

        assertTrue(clock.compareTo(clock2) == CMP_CLOCK.CMP_DOMINATES);
        assertTrue(clock2.compareTo(clock) == CMP_CLOCK.CMP_ISDOMINATED);
    }

    @Test
    public void testDominates2() {
        // Prepare [5 \ {2,4}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA5);
        clock.record(tsA3);
        // Prepare [5 \ {2,4}, 0] vector.
        clock2.record(tsA1);
        clock2.record(tsA3);
        clock2.record(tsA5);

        assertTrue(clock.compareTo(clock2) == CMP_CLOCK.CMP_EQUALS);
        assertTrue(clock2.compareTo(clock) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testDominates3() {
        // Prepare [2, 0] vector.
        clock.record(tsA1);
        clock.record(tsA2);
        // Prepare [3 \ {1}, 0] vector.
        clock2.record(tsA2);
        clock2.record(tsA3);

        assertTrue(clock.compareTo(clock2) == CMP_CLOCK.CMP_CONCURRENT);
        assertTrue(clock2.compareTo(clock) == CMP_CLOCK.CMP_CONCURRENT);
    }

    @Test
    public void testDominates4() {
        // Prepare [3, 0] vector.
        clock.record(tsA1);
        clock.record(tsA2);
        clock.record(tsA3);
        // Prepare [3 \ {1}, 0] vector.
        clock2.record(tsA2);
        clock2.record(tsA3);

        assertTrue(clock.compareTo(clock2) == CMP_CLOCK.CMP_DOMINATES);
        assertTrue(clock2.compareTo(clock) == CMP_CLOCK.CMP_ISDOMINATED);
    }

    @Test
    public void testMerge1() {
        // Prepare [5 \ {2,4}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA5);
        clock.record(tsA3);
        // Prepare [5 \ {2,3,4}, 0] vector.
        clock2.record(tsA1);
        clock2.record(tsA5);
        // Prepare [5 \ {2,4}, 0] vector.
        clock3.record(tsA1);
        clock3.record(tsA5);
        clock3.record(tsA3);

        assertTrue(clock.merge(clock2) == CMP_CLOCK.CMP_DOMINATES);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testMerge2() {
        // Prepare [5 \ {2,4}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA5);
        clock.record(tsA3);
        // Prepare [5 \ {2,4}, 0] vector.
        clock2.record(tsA1);
        clock2.record(tsA3);
        clock2.record(tsA5);
        // Prepare [5 \ {2,4}, 0] vector.
        clock3.record(tsA1);
        clock3.record(tsA5);
        clock3.record(tsA3);

        assertTrue(clock.merge(clock2) == CMP_CLOCK.CMP_EQUALS);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testMerge3() {
        // Prepare [2, 0] vector.
        clock.record(tsA1);
        clock.record(tsA2);
        // Prepare [3 \ {1}, 0] vector.
        clock2.record(tsA2);
        clock2.record(tsA3);
        // Prepare [3, 0] vector.
        clock3.record(tsA1);
        clock3.record(tsA2);
        clock3.record(tsA3);

        assertTrue(clock.merge(clock2) == CMP_CLOCK.CMP_CONCURRENT);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testMerge4() {
        // Prepare [3, 0] vector.
        clock.record(tsA1);
        clock.record(tsA2);
        clock.record(tsA3);
        // Prepare [3 \ {1}, 0] vector.
        clock2.record(tsA2);
        clock2.record(tsA3);
        // Prepare [5 \ {2,4}, 0] vector.
        clock3.record(tsA1);
        clock3.record(tsA2);
        clock3.record(tsA3);

        assertTrue(clock.merge(clock2) == CMP_CLOCK.CMP_DOMINATES);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testAddInDifferentOrder() {
        // Prepare [4 \ {2}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA3);
        clock.record(tsA4);
        // Prepare [4 \ {2}, 0] vector.
        clock2.record(tsA1);
        clock2.record(tsA4);
        clock2.record(tsA3);
        // Prepare [4 \ {2}, 0] vector.
        clock3.record(tsA4);
        clock3.record(tsA3);
        clock3.record(tsA1);

        assertTrue(clock.compareTo(clock2) == CMP_CLOCK.CMP_EQUALS);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
        assertTrue(clock2.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testIntersect1() {
        // Prepare [4 \ {2}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA3);
        clock.record(tsA4);
        // Prepare[2\{1}, 0] vector.
        clock2.record(tsA2);
        // Prepare[0, 0] vector.

        assertTrue(clock.intersect(clock2) == CMP_CLOCK.CMP_CONCURRENT);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testIntersect2() {
        // Prepare [4 \ {2}, 0] vector.
        clock.record(tsA1);
        clock.record(tsA3);
        clock.record(tsA4);
        // Prepare[4\{1,3}, 0] vector.
        clock2.record(tsA2);
        clock2.record(tsA4);
        // Prepare[4\{1,2,3}, 0] vector.
        clock3.record(tsA4);

        assertTrue(clock.intersect(clock2) == CMP_CLOCK.CMP_CONCURRENT);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testIntersect3() {
        // Prepare [4 \ {2}, 1] vector.
        clock.record(tsA1);
        clock.record(tsA3);
        clock.record(tsA4);
        clock.record(tsB1);
        // Prepare[4\{1,3}, 1] vector.
        clock2.record(tsA2);
        clock2.record(tsA4);
        clock2.record(tsB1);
        // Prepare[4\{1,2,3}, 1] vector.
        clock3.record(tsA4);
        clock3.record(tsB1);

        assertTrue(clock.intersect(clock2) == CMP_CLOCK.CMP_CONCURRENT);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }

    @Test
    public void testIntersect4() {
        // Prepare [3, 2] vector.
        clock.record(tsA1);
        clock.record(tsA2);
        clock.record(tsA3);
        clock.record(tsB1);
        clock.record(tsB2);
        // Prepare[2, 3] vector.
        clock2.record(tsA1);
        clock2.record(tsA2);
        clock2.record(tsB1);
        clock2.record(tsB2);
        clock2.record(tsB3);
        // Prepare[2, 2] vector.
        clock3.record(tsA1);
        clock3.record(tsA2);
        clock3.record(tsB1);
        clock3.record(tsB2);

        assertTrue(clock.intersect(clock2) == CMP_CLOCK.CMP_CONCURRENT);
        assertTrue(clock.compareTo(clock3) == CMP_CLOCK.CMP_EQUALS);
    }
}
