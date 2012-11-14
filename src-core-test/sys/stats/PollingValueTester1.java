package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import sys.stats.common.PlaneValues;
import sys.stats.common.PlotValues;
import sys.stats.sources.PollingBasedValueProvider;

public class PollingValueTester1 {

    @Test
    public void testpolling() {
        Stats.init(1000);

        UpdatingFieldClass classWithField = new UpdatingFieldClass(1000);

        Stats.registerPollingBasedValueProvider("poll", classWithField.getPoller());

        try {
            Thread.sleep(5500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, PlotValues<Long, Double>> pollingSummary = Stats.getPollingSummary();

        System.out.println(pollingSummary);

        PlotValues<Long, Double> pollingValues = pollingSummary.get("poll");
        int increment = 1;
        int expected = 0;
        Iterator<PlaneValues<Long, Double>> it = pollingValues.getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValues<Long, Double> v = it.next();
            assertEquals(expected, v.getY().intValue());
            expected += increment;
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
