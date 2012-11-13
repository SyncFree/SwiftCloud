package sys.stats.statisticsOverTime;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.slices.CounterImpl;
import sys.stats.sources.CounterSignalSource;

public class CounterOverTime extends GenericStatisticsOverTime<CounterImpl> implements CounterSignalSource {

    public CounterOverTime(long timeSlice, String sourceName) {
        super(timeSlice, new CounterImpl());
    }

    @Override
    public void incCounter() {
        CounterImpl slice = getCurrentSlice(System.currentTimeMillis());
        slice.incCounter();
    }

    public int getTotalCount() {
        int count = 0;
        List<Pair<Long, CounterImpl>> slices = getAllSlices();
        for (Pair<Long, CounterImpl> s : slices) {
            count += s.getSecond().getTotalOperations();
        }
        return count;
    }

    @Override
    public PlotValues<Long, Integer> getPlotValues() {
        List<Pair<Long, CounterImpl>> slices = getAllSlices();
        PlotValues<Long, Integer> plotValues = new PlotValues<Long, Integer>();
        for (Pair<Long, CounterImpl> v : slices) {
            plotValues.addValue(v.getFirst(), v.getSecond().getTotalOperations());
        }
        return plotValues;
    }

}
