package sys.stats;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.SliceStatistics.GenericStatisticsOverTime;
import sys.stats.common.SliceStatistics.HistogramImpl;
import sys.stats.common.SliceStatistics.SimpleValueSignalImpl;

public class ValuesOverTime extends GenericStatisticsOverTime<SimpleValueSignalImpl> implements ValuesSignal {

    public ValuesOverTime(long timeSlice, String sourceName) {
        super(timeSlice, new SimpleValueSignalImpl());
    }

    @Override
    public synchronized void recordSignal(double value) {
        SimpleValueSignalImpl slice = getCurrentSlice(System.currentTimeMillis());
        slice.setValue(value);

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
    public synchronized PlotValues<Long, Double> getPlotValues() {
        List<Pair<Long, SimpleValueSignalImpl>> slices = getAllSlices();
        PlotValues<Long, Double> plotValues = new PlotValues<Long, Double>();
        for (Pair<Long, SimpleValueSignalImpl> v : slices) {
            plotValues.addValue(v.getFirst(), v.getSecond().getValue());
        }
        return plotValues;
    }

}
