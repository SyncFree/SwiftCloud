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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TimestampTest {

    @Test
    public void cloneTest() {
        IncrementalTimestampGenerator gen = new IncrementalTimestampGenerator("s1");
        Timestamp t1 = gen.generateNew();
        Timestamp t2 = t1.clone();

        assertTrue(t1.equals(t2));
        assertTrue(t1.includes(t2));
    }

    @Test
    public void diffIdsTest() {
        IncrementalTimestampGenerator gen1 = new IncrementalTimestampGenerator("s1");
        Timestamp t1 = gen1.generateNew();
        IncrementalTimestampGenerator gen2 = new IncrementalTimestampGenerator("s2");
        Timestamp t2 = gen2.generateNew();

        assertTrue(!t1.equals(t2));
        assertTrue(!t1.includes(t2));
    }

}
