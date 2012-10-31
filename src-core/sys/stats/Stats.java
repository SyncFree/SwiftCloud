package sys.stats;

public class Stats {
    // Make it configurable via init() or properties.
    public static final int SAMPLING_INTERVAL_MILLIS = 10000;

    public static CountingSource getCountingSourceForStat(String statName) {
        return null;
    }

    public static ValuesSource getValuesSourceForStat(String statName, Double... exportedPercentiles) {
        return null;
    }

    public static void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider) {

    }
}
