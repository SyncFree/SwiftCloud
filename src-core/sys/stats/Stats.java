package sys.stats;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.utils.Pair;

public class Stats {
    // Make it configurable via init() or properties.
    public static final int SAMPLING_INTERVAL_MILLIS = 1000;

    private static Map<String, CountingSource> countigSources;
    private static Map<String, ValuesSource> valuesSources;
    private static Map<String, Pair<ValuesSource, PollingBasedValueProvider>> pollingProviders;

    private static Thread pollWorker;

    public static void init() {
        countigSources = new HashMap<String, CountingSource>();
        valuesSources = new HashMap<String, ValuesSource>();
        pollingProviders = new LinkedHashMap<String, Pair<ValuesSource, PollingBasedValueProvider>>();

        pollWorker = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        for (Entry<String, Pair<ValuesSource, PollingBasedValueProvider>> p : pollingProviders
                                .entrySet()) {
                            Pair<ValuesSource, PollingBasedValueProvider> pollStats = p.getValue();
                            double value = pollStats.getSecond().poll();
                            pollStats.getFirst().recordEventWithValue(value);
                        }
                        Thread.sleep(SAMPLING_INTERVAL_MILLIS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        pollWorker.setDaemon(true);
    }

    public static CountingSource getCountingSourceForStat(String statName) {
        CountingSource cs = null;
        synchronized (countigSources) {
            cs = countigSources.get(statName);
            if (cs == null)
                countigSources.put(statName, new CountingSourceImpl(statName, SAMPLING_INTERVAL_MILLIS));
        }
        return cs;
    }

    // TODO: Must think about this
    public static ValuesSource getValuesSourceForStat(String statName, Double... exportedPercentiles) {
        ValuesSource vs = null;
        synchronized (valuesSources) {
            vs = valuesSources.get(statName);
            if (vs == null)
                valuesSources.put(statName, new ValueSourceImpl(statName, SAMPLING_INTERVAL_MILLIS));
        }
        return vs;
    }

    public static void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider) {
        Pair<ValuesSource, PollingBasedValueProvider> ps = null;
        synchronized (pollingProviders) {
            ps = pollingProviders.get(statName);
            if (ps == null)
                pollingProviders.put(statName, new Pair<ValuesSource, PollingBasedValueProvider>(new ValueSourceImpl(
                        statName, SAMPLING_INTERVAL_MILLIS), provider));
            if (!pollWorker.isAlive())
                pollWorker.start();
        }
    }

    public static Map<String, List<Pair<Long, Double>>> getPollingSummary() {
        LinkedHashMap<String, List<Pair<Long, Double>>> pollingSummary = new LinkedHashMap<String, List<Pair<Long, Double>>>();
        Set<Entry<String, Pair<ValuesSource, PollingBasedValueProvider>>> values = pollingProviders.entrySet();
        for (Entry<String, Pair<ValuesSource, PollingBasedValueProvider>> v : values) {
            // TODO: Abstarct this
            pollingSummary.put(v.getKey(), ((ValueSourceImpl) v.getValue().getFirst()).getSumOverTime());

        }
        return pollingSummary;
    }
}
