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

import sys.stats.overtime.ValueOverTime;
import sys.stats.sources.ValueSignalSource.Stopper;

public class ValueSourceTester {

    @Test
    public void testOpLatency1() {

        ValueOverTime opsLatency = new ValueOverTime(200, "test");
        Stopper stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        stopper.stop();
        // Latency should be under 200ms.
        System.out.println(opsLatency.getPlotValues());
        Iterator<PlaneValues<Long, Double>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        PlaneValues<Long, Double> value = it.next();
        assertEquals(0, (long) value.getX());
        value = it.next();
        assertEquals(200, (long) value.getX());

    }

    @Test
    public void testOpLatency2() {
        ValueOverTime opsLatency = new ValueOverTime(200, "test");
        Stopper stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();
        System.out.println(opsLatency.getPlotValues());
        Iterator<PlaneValues<Long, Double>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        PlaneValues<Long, Double> value = it.next();
        assertEquals(0, (long) value.getX());
        // Latency should be under 600ms and should not have any empty value
        // between 0 and 600
        value = it.next();
        assertEquals(600, (long) value.getX());

    }

}
