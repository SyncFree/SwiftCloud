package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import sys.stats.common.PlaneValues;
import sys.stats.slicedStatistics.slices.histogram.Histogram;
import sys.stats.sources.ValueSignalSource.Stopper;
import sys.stats.statisticsOverTime.ValueOverTime;

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
