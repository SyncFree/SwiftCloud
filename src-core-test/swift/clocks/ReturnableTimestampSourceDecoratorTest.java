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
