package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import sys.stats.common.PlaneValues;
import sys.stats.common.PlotValues;
import sys.stats.sources.PollingBasedValueProvider;

public class PollingValueTester2 {

    @Test
    public void testpolling2() {

        Stats.init(5000);
        
        UpdatingFieldClass classWithField = new UpdatingFieldClass(1000);

        Stats.registerPollingBasedValueProvider("poll2", classWithField.getPoller());

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, PlotValues<Long, Double>> pollingSummary = Stats.getPollingSummary();

        System.out.println(pollingSummary);

        PlotValues<Long, Double> pollingValues = pollingSummary.get("poll2");
        Iterator<PlaneValues<Long, Double>> it = pollingValues.getPlotValuesIterator();
        PlaneValues<Long, Double> v = it.next();
        assertEquals(0, v.getY().intValue());
        v = it.next();
        assertEquals(5, v.getY().intValue());

        Stats.dispose();
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
