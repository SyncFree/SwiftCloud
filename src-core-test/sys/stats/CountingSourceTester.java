package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import sys.stats.common.PlaneValues;
import sys.stats.common.PlotValues;
import sys.stats.statisticsOverTime.CounterOverTime;

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

        Iterator<PlaneValues<Long, Integer>> it = values.getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValues<Long, Integer> value = it.next();
            if (value.getX() >= 1000l)
                sum += value.getY().intValue();
        }
        assertEquals(200, sum);
    }

}
