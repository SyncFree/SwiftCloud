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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import sys.stats.common.PlaneValue;
import sys.stats.common.PlotValues;
import sys.stats.sources.PollingBasedValueProvider;

public class PollingValueTester2 {

    @Test
    public void testpolling2() {

        StatsImpl stats = StatsImpl.getInstance("teste");

        UpdatingFieldClass classWithField = new UpdatingFieldClass(1000);

        stats.registerPollingBasedValueProvider("poll2", classWithField.getPoller(), 4999);

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, PlotValues<Long, Double>> pollingSummary = stats.getPollingSummary();

        System.out.println(pollingSummary);

        PlotValues<Long, Double> pollingValues = pollingSummary.get("poll2");
        Iterator<PlaneValue<Long, Double>> it = pollingValues.getPlotValuesIterator();
        PlaneValue<Long, Double> v = it.next();
        assertEquals(0, v.getY().intValue());
        v = it.next();
        assertEquals(5, v.getY().intValue());

        try {
            stats.outputAndDispose();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    class UpdatingFieldClass {
        private int theField;

        public UpdatingFieldClass(final int frequency) {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (true) {
                        theField++;
                        System.out.println(theField);
                        try {
                            Thread.sleep(frequency);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }

        public PollingBasedValueProvider getPoller() {
            return new PollingBasedValueProvider() {

                @Override
                public double poll() {
                    return theField;
                }
            };
        }
    }

}