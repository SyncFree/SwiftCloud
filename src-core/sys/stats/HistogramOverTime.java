package sys.stats;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.SliceStatistics.CommDistributionImpl;
import sys.stats.common.SliceStatistics.CounterSignalImpl;
import sys.stats.common.SliceStatistics.GenericStatisticsOverTime;
import sys.stats.common.SliceStatistics.Histogram;
import sys.stats.common.SliceStatistics.HistogramImpl;

public class HistogramOverTime extends GenericStatisticsOverTime<CommDistributionImpl> implements ValuesSignal {

    public HistogramOverTime(long timeSlice, double[] commValues, String sourceName) {
        super(timeSlice, new CommDistributionImpl(sourceName, commValues));
    }

    @Override
    public void recordSignal(double value) {
        CommDistributionImpl slice = getCurrentSlice(System.currentTimeMillis());
        slice.addValue(value);

    }

    @Override
    public Stopper createEventDurationSignal() {
        final long TS = System.currentTimeMillis();
        Stopper stopper = new Stopper() {
            public void stop() {
                long TE = System.currentTimeMillis();
                long duration = TE - TS;
                recordSignal(duration);
            }

        };
        return stopper;

    }

    @Override
    public PlotValues<Long, Histogram> getPlotValues() {
        List<Pair<Long, CommDistributionImpl>> slices = getAllSlices();
        PlotValues<Long, Histogram> histogram = new PlotValues<Long, Histogram>();
        for (Pair<Long, CommDistributionImpl> s : slices) {
            histogram.addValue(s.getFirst(), s.getSecond());
        }
        return histogram;
    }

}
