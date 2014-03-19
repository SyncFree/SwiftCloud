package sys.stats;

import java.io.IOException;

import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.PollingBasedValueProvider;
import sys.stats.sources.ValueSignalSource;

/**
 * Collects statistics for different types of sources over time. Enables
 * collecting values opportunistically, or periodically, with different
 * semantics.
 * 
 * @author balegas,mzawirsk
 * @see StatsImpl,DummyStats
 */
public interface Stats {
    /**
     * Assigns a new polling based statistics gatherer to this statistics
     * manager
     * 
     * @param statName
     *            the name of the polling based value provider
     * @param provider
     *            the implementation of the provider
     * @param frequency
     */
    public abstract void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider,
            int frequency);

    /**
     * Returns an empty ValueSignalSource with the given name, or an already
     * existing one with the gathered values, ignoring the requested bins.
     * 
     * @param statName
     *            the name of the ValueSignalSource
     * @param valueBins
     *            an array containing the values to store the frequency. Values
     *            in-between are assigned to the greatest specified.
     * @return CounterSignalSource
     */
    public abstract ValueSignalSource getValuesFrequencyOverTime(String statName, double... valueBins);

    /**
     * Returns a CounterSignalSource with the given name and value 0, or an
     * already existing one with the current value.
     * 
     * @param statName
     *            the name of the counter
     * @return CounterSignalSource
     */
    public abstract CounterSignalSource getCountingSourceForStat(String statName);

    /**
     * Writes the gathered statistics to the output folder since the creation of
     * the statistics manager. Probe names with ":" are split and created in
     * sub-folders accordingly
     * 
     * @throws IOException
     */
    public abstract void dump() throws IOException;

    /**
     * Stops statistics collecting threads.
     */
    public abstract void terminate();
}
