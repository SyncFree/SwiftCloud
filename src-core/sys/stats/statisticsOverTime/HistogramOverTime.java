package sys.stats.statisticsOverTime;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.slices.histogram.CommDistributionImpl;
import sys.stats.slicedStatistics.slices.histogram.Histogram;
import sys.stats.sources.ValueSignalSource;

public class HistogramOverTime extends GenericStatisticsOverTime<CommDistributionImpl> implements ValueSignalSource {

    public HistogramOverTime(long timeSlice, double[] commValues, String sourceName) {
        super(timeSlice, new CommDistributionImpl(sourceName, commValues));
    }

    @Override
    public void setValue(double value) {
        CommDistributionImpl slice = getCurrentSlice();
        slice.addValue(value);

    }

    @Override
    public Stopper createEventDurationSignal() {
        final long TS = System.currentTimeMillis();
        Stopper stopper = new Stopper() {
            public void stop() {
                long TE = System.currentTimeMillis();
                long duration = TE - TS;
                setValue(duration);
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