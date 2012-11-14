package sys.stats.statisticsOverTime;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.slicedStatistics.slices.ValueImpl;
import sys.stats.sources.ValueSignalSource;

public class ConstantRateValueOverTime extends GenericStatisticsOverTime<ValueImpl> implements ValueSignalSource {

    public ConstantRateValueOverTime(long timeSlice, String sourceName) {
        super(timeSlice, new ValueImpl());
    }

    @Override
    public synchronized void setValue(double value) {
        long currTime = System.currentTimeMillis()-T0;
        List<Pair<Long, ValueImpl>> allSlices = getAllSlices();
        if (allSlices.get(allSlices.size() - 1).getFirst() - currTime == 0)
            allSlices.get(allSlices.size() - 1).getSecond().setValue(value);
        else {
            ValueImpl slice = addSliceAndReturn();
            slice.setValue(value);
        }

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
    public synchronized PlotValues<Long, Double> getPlotValues() {
        List<Pair<Long, ValueImpl>> slices = getAllSlices();
        PlotValues<Long, Double> plotValues = new PlotValues<Long, Double>();
        for (Pair<Long, ValueImpl> v : slices) {
            plotValues.addValue(v.getFirst(), v.getSecond().getValue());
        }
        return plotValues;
    }

}
