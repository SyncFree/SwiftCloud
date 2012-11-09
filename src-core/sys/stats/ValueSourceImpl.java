package sys.stats;

import java.util.List;

import swift.utils.Pair;
import sys.stats.common.BinnedTally;

public class ValueSourceImpl extends AbstractTallyStatistics implements ValuesSource {

    private long T0 = 0;

    public ValueSourceImpl(String sourceName, long latencyTimespanMillis) {
        this.opsRecorder = new BinnedTally(latencyTimespanMillis, sourceName);
        this.timespanMillis = latencyTimespanMillis;
        this.sourceName = sourceName;
    }

    //TODO: Not sure where we will use this, seems more like an abstraction for CountingSource
    @Override
    public void recordEventWithValue(double value) {
        if (T0 == 0) {
            T0 = System.currentTimeMillis();
        }
        long TE = System.currentTimeMillis();
        long duration = TE - T0;
        opsRecorder.tally(duration, value);

    }

    @Override
    public Stopper createEventRecordingStopper() {
        final long TS = System.currentTimeMillis();
        Stopper stopper = new Stopper() {
            public void stop() {
                long TE = System.currentTimeMillis();
                long duration = TE - TS;
                opsRecorder.tally(duration, 1);
            }

        };
        return stopper;

    }

    public List<Pair<Long, Double>> getLatencyHistogram() {
        return getSumOverTime();
    }

    public List<Pair<Long, Double>> getLatencyHistogram(long latencyTimespanMillis) {
        return getSumOverTime(latencyTimespanMillis);
    }
    
    public List<Pair<Long, Double>> getAverageOverTime() {
        return super.getAverageOverTime();
    }

}
