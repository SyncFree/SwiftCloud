package sys.stats;

import java.util.List;

import swift.utils.Pair;

public interface CountingSource {
    public void incCounter();

    public long getElapsedTimeMillis();

    public List<Pair<Long, Double>> getCountOverTime();

    public List<Pair<Long, Double>> getCountOverTime(long frequencyMillis);
}
