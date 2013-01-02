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
package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import sys.stats.common.PlaneValue;
import sys.stats.common.PlotValues;
import sys.stats.overtime.CounterOverTime;

public class CountingSourceTester {

    @Test
    public void testNumOps() {

        CounterOverTime counter = new CounterOverTime(200, "test");
        for (int i = 0; i < 10; i++) {
            counter.incCounter();
        }

        assertEquals(10, counter.getTotalCount());
    }

    @Test
    // Test if the Counter stores operations according to the time they were
    // executed
    public void testOpsMoreThan1Second() {

        CounterOverTime counter = new CounterOverTime(200, "test");
        for (int i = 0; i < 10; i++) {
            counter.incCounter();
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 200; i++) {
            counter.incCounter();
        }

        PlotValues<Long, Integer> values = counter.getPlotValues();
        int sum = 0;

        Iterator<PlaneValue<Long, Integer>> it = values.getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValue<Long, Integer> value = it.next();
            if (value.getX() >= 1000l)
                sum += value.getY().intValue();
        }
        assertEquals(200, sum);
    }

}