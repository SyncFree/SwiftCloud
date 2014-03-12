package sys.stats;

import java.io.IOException;

import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.PollingBasedValueProvider;
import sys.stats.sources.ValueSignalSource;

/**
 * @see StatsImpl
 * @author mzawirsk
 */
public interface Stats {
    public abstract void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider,
            int frequency);

    public abstract ValueSignalSource getValuesFrequencyOverTime(String statName, double... valueBins);

    public abstract CounterSignalSource getCountingSourceForStat(String statName);

    public abstract void outputAndDispose() throws IOException;
}
