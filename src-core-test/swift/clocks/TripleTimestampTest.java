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

import org.junit.Test;

/**
 * @author mzawirski
 */
public class TripleTimestampTest {
    @Test
    public void testCompareEquals() {
        final IncrementalTripleTimestampGenerator gen = new IncrementalTripleTimestampGenerator(
                new Timestamp("a", 1));
        final TripleTimestamp a = gen.generateNew();
        final TripleTimestamp b = gen.generateNew();

        assertFalse(a.equals(b));
        assertFalse(a.compareTo(b) == 0);
        assertTrue(a.equals(a));
        assertTrue(a.compareTo(a) == 0);
    }
}
