package sys.stats;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import sys.stats.HistogramOverTime;
import sys.stats.Stats;
import sys.stats.TallyHistogramOverTime;
import sys.stats.PlaneValues;
import sys.stats.PlotValues;
import sys.stats.ValuesOverTime;
import sys.stats.ValuesSignal;
import sys.stats.ValuesSignal.Stopper;
import sys.stats.common.SliceStatistics.Histogram;

public class HistogramTester {

    @Test
    public void testOpLatency1() {
        Stats.init();

        HistogramOverTime opsLatency = Stats.getValuesFrequencyOverTime("histogram", 200, 400, 1000);

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
                if (planeValues.getX() == 1000)
                    assertEquals(2, (int) planeValues.getY());
            }
        }

    }

    // Experiencia que dura mais tempo do que apenas um slice. Os resutlados n
    // podem aparecer agregados, mas devem fazer referencia aos mesmos valores.
    // Depois fazer o merge de dois slices
    @Test
    public void testOpLatency2() {
        Stats.init(1300);

        HistogramOverTime opsLatency = Stats.getValuesFrequencyOverTime("histogram", 200, 400, 1000);

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
        Iterator<PlaneValues<Long, Histogram>> it = opsLatency.getPlotValues().getPlotValuesIterator();
        // The Histograms iterator
        while (it.hasNext()) {
            PlaneValues<Long, Histogram> value = it.next();
            Iterator<PlaneValues<Double, Integer>> histogram = value.getY().getHistogram().getPlotValuesIterator();
            System.out.println(value);
            // The values of the histogram
            while (histogram.hasNext()) {
                PlaneValues<Double, Integer> planeValues = histogram.next();
                if (planeValues.getX() == 1300)
                    assertEquals(2, (int) planeValues.getY());
                if (planeValues.getX() == 2600)
                    assertEquals(1, (int) planeValues.getY());
            }
        }

    }

}
