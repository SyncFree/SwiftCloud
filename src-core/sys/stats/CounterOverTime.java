package sys.stats;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.SliceStatistics.CounterSignalImpl;
import sys.stats.common.SliceStatistics.GenericStatisticsOverTime;
import sys.stats.common.SliceStatistics.SimpleValueSignalImpl;

public class CounterOverTime extends GenericStatisticsOverTime<CounterSignalImpl> implements CounterSignal {

    public CounterOverTime(long timeSlice, String sourceName) {
        super(timeSlice, new CounterSignalImpl());
    }

    @Override
    public void incCounter() {
        CounterSignalImpl slice = getCurrentSlice(System.currentTimeMillis());
        slice.incCounter();
    }

    public int getTotalCount() {
        int count = 0;
        List<Pair<Long, CounterSignalImpl>> slices = getAllSlices();
        for (Pair<Long, CounterSignalImpl> s : slices) {
            count += s.getSecond().getTotalOperations();
        }
        return count;
    }

    @Override
    public PlotValues<Long, Integer> getPlotValues() {
        List<Pair<Long, CounterSignalImpl>> slices = getAllSlices();
        PlotValues<Long, Integer> plotValues = new PlotValues<Long, Integer>();
        for (Pair<Long, CounterSignalImpl> v : slices) {
            plotValues.addValue(v.getFirst(), v.getSecond().getTotalOperations());
        }
        return plotValues;
    }

}
