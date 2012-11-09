package sys.stats;

import java.util.List;

import swift.utils.Pair;

public interface ValuesSource {
    public void recordEventWithValue(double value);

    public Stopper createEventRecordingStopper();

    public List<Pair<Long, Double>> getLatencyHistogram();

    public List<Pair<Long, Double>> getLatencyHistogram(long latencyTimespanMillis);

    public List<Pair<Long, Double>> getAverageOverTime();

    interface Stopper {
        void stop();
    }
}
