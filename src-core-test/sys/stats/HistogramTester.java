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
import sys.stats.overtime.HistogramOverTime;
import sys.stats.sliced.slices.histogram.Histogram;
import sys.stats.sources.ValueSignalSource.Stopper;

public class HistogramTester {

    @Test
    public void testOpLatency1() {
        Stats stats = Stats.getInstance("test");

        HistogramOverTime opsLatency = (HistogramOverTime) stats
                .getValuesFrequencyOverTime("histogram", 200, 400, 1000);

        Stopper stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        System.out.println(opsLatency.getPlotValues());

        // Latency should be under 600ms for successive operations.
        Iterator<PlaneValue<Long, Histogram>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        it.next();
        // The Histograms iterator
        while (it.hasNext()) {
            PlaneValue<Long, Histogram> value = it.next();
            Iterator<PlaneValue<Double, Integer>> histogram = value.getY().getHistogram().getPlotValuesIterator();
            // The values of the histogram
            while (histogram.hasNext()) {
                PlaneValue<Double, Integer> planeValues = histogram.next();
                if (planeValues.getX() == 1000)
                    assertEquals(2, (int) planeValues.getY());
            }
        }

    }

    // Experiencia que dura mais tempo do que apenas um slice. Os resutlados n
    // podem aparecer agregados, mas devem fazer referencia aos mesmos valores.
    // Depois fazer o merge de dois slices
    // @Test
    public void testOpLatency2() {

        Stats stats = Stats.getInstance("test2");

        HistogramOverTime opsLatency = (HistogramOverTime) stats
                .getValuesFrequencyOverTime("histogram", 200, 400, 1000);

        Stopper stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();

        // Latency should be under 600ms for successive operations.
        Iterator<PlaneValue<Long, Histogram>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        // The Histograms iterator
        while (it.hasNext()) {
            PlaneValue<Long, Histogram> value = it.next();
            Iterator<PlaneValue<Double, Integer>> histogram = value.getY().getHistogram().getPlotValuesIterator();
            System.out.println(value);
            // The values of the histogram
            while (histogram.hasNext()) {
                PlaneValue<Double, Integer> planeValues = histogram.next();
                if (planeValues.getX() == 1300)
                    assertEquals(2, (int) planeValues.getY());
                if (planeValues.getX() == 2600)
                    assertEquals(1, (int) planeValues.getY());
            }
        }

    }

}