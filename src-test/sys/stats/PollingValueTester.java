package sys.stats;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import swift.utils.Pair;
import sys.stats.PlaneValues;
import sys.stats.PlotValues;
import sys.stats.PollingBasedValueProvider;
import sys.stats.Stats;

public class PollingValueTester {

    @Test
    public void testpolling() {
        Stats.init(1000);

        UpdatingFieldClass classWithField = new UpdatingFieldClass(1000);
        UpdatingFieldClass classWithField2 = new UpdatingFieldClass(500);


        Stats.registerPollingBasedValueProvider("poll", classWithField.getPoller());
        Stats.registerPollingBasedValueProvider("poll2", classWithField2.getPoller());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, PlotValues<Long, Double>> pollingSummary = Stats.getPollingSummary();

        System.out.println(pollingSummary);

        PlotValues<Long, Double> pollingValues = pollingSummary.get("poll");
        int increment = 1;
        int expected = increment;
        Iterator<PlaneValues<Long, Double>> it = pollingValues.getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValues<Long, Double> v = it.next();
            assertEquals(expected, v.getY().intValue());
            expected += increment;
        }

        pollingValues = pollingSummary.get("poll2");
        increment = 2;
        expected = 1;

        it = pollingValues.getPlotValuesIterator();
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
                        // System.out.println(theField);
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
