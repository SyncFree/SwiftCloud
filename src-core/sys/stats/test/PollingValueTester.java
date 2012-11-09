package sys.stats.test;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import swift.utils.Pair;
import sys.stats.PollingBasedValueProvider;
import sys.stats.Stats;

public class PollingValueTester {

    @Test
    public void testpolling() {

        UpdatingFieldClass classWithField = new UpdatingFieldClass(1000);
        UpdatingFieldClass classWithField2 = new UpdatingFieldClass(500);

        Stats.init();

        Stats.registerPollingBasedValueProvider("poll", classWithField.getPoller());
        Stats.registerPollingBasedValueProvider("poll2", classWithField2.getPoller());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, List<Pair<Long, Double>>> pollingSummary = Stats.getPollingSummary();
        
        System.out.println(pollingSummary);

        List<Pair<Long, Double>> pollingValues = pollingSummary.get("poll");
        int increment = 1;
        int expected = increment;
        for (Pair<Long, Double> v : pollingValues) {
            assertEquals(expected, v.getSecond().intValue());
            expected += increment;
        }

        pollingValues = pollingSummary.get("poll2");
        increment = 2;
        expected = 1;
        for (Pair<Long, Double> v : pollingValues) {
            assertEquals(expected, v.getSecond().intValue());
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
