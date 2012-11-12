package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import sys.stats.TallyHistogramOverTime;
import sys.stats.PlaneValues;
import sys.stats.PlotValues;
import sys.stats.ValuesOverTime;
import sys.stats.ValuesSignal.Stopper;
import sys.stats.common.SliceStatistics.Histogram;

public class ValueSourceTester {

    @Test
    public void testOpLatency1() {

        ValuesOverTime opsLatency = new ValuesOverTime(200, "test");
        Stopper stopper = opsLatency.createEventDurationSignal();
        stopper.stop();
        // Latency should be under 200ms.
        Iterator<PlaneValues<Long, Double>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValues<Long, Double> value = it.next();
            long opDuration = value.getX();
            assertEquals(200, opDuration);
        }

    }

    @Test
    public void testOpLatency2() {
        ValuesOverTime opsLatency = new ValuesOverTime(200, "test");
        Stopper stopper = opsLatency.createEventDurationSignal();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopper.stop();
        // Latency should be under 600ms.
        System.out.println(opsLatency.getPlotValues());
        Iterator<PlaneValues<Long, Double>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValues<Long, Double> value = it.next();
            long opDuration = value.getX();
            if (value.getY().intValue() > 0)
                assertEquals(600, opDuration);
            else
                assertEquals(0, value.getY().intValue());
        }

    }

    @Test
    public void testOpLatency3() {
        TallyHistogramOverTime opsLatency = new TallyHistogramOverTime(1200, 200, "test");
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

        // Latency should be under 600ms for successive operations.
        Iterator<PlaneValues<Long, Histogram>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        // The Histograms iterator
        while (it.hasNext()) {
            PlaneValues<Long, Histogram> value = it.next();
            Iterator<PlaneValues<Double, Integer>> histogram = value.getY().getHistogram().getPlotValuesIterator();
            // The values of the histogram
            while (histogram.hasNext()) {
                PlaneValues<Double, Integer> planeValues = histogram.next();
                if (planeValues.getX() == 600)
                    assertEquals(2, (int) planeValues.getY());
            }
        }

    }
}
