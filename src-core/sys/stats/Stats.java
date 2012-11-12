package sys.stats;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.utils.Pair;
import sys.stats.common.SliceStatistics.SimpleValueSignalImpl;

public class Stats {
    public static final int SAMPLING_INTERVAL_MILLIS = 10000;

    private static Map<String, CounterOverTime> countigSources;
    // private static Map<String, ValuesOverTime> valuesSources;
    private static Map<String, HistogramOverTime> valuesFrequencySource;
    private static Map<String, Pair<ValuesOverTime, PollingBasedValueProvider>> pollingProviders;
    private static int currentSamplingInterval;

    private static Thread pollWorker;

    public static void init() {
        init(SAMPLING_INTERVAL_MILLIS);
    }

    public static void init(int samplingInterval) {
        currentSamplingInterval = samplingInterval;
        countigSources = new HashMap<String, CounterOverTime>();
        // valuesSources = new HashMap<String, ValuesOverTime>();
        valuesFrequencySource = new HashMap<String, HistogramOverTime>();
        pollingProviders = new LinkedHashMap<String, Pair<ValuesOverTime, PollingBasedValueProvider>>();

        pollWorker = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        for (Entry<String, Pair<ValuesOverTime, PollingBasedValueProvider>> p : pollingProviders
                                .entrySet()) {
                            Pair<ValuesOverTime, PollingBasedValueProvider> pollStats = p.getValue();
                            double value = pollStats.getSecond().poll();
                            pollStats.getFirst().recordSignal(value);
                        }
                        Thread.sleep(currentSamplingInterval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        pollWorker.setDaemon(true);
    }

    public static CounterSignal getCountingSourceForStat(String statName) {
        CounterOverTime cs = null;
        synchronized (countigSources) {
            cs = countigSources.get(statName);
            if (cs == null) {
                cs = new CounterOverTime(currentSamplingInterval, statName);
                countigSources.put(statName, cs);
            }
        }
        return cs;
    }

    // TODO: Must abstract HistogramInterface
    public static HistogramOverTime getValuesFrequencyOverTime(String statName, double... histogramIntervals) {
        HistogramOverTime hist = null;
        synchronized (valuesFrequencySource) {
            hist = valuesFrequencySource.get(statName);
            if (hist == null) {
                hist = new HistogramOverTime(currentSamplingInterval, histogramIntervals, statName);
                valuesFrequencySource.put(statName, hist);
            }
        }
        return hist;
    }

    public static void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider) {
        Pair<ValuesOverTime, PollingBasedValueProvider> ps = null;
        synchronized (pollingProviders) {
            ps = pollingProviders.get(statName);
            if (ps == null)
                pollingProviders.put(statName, new Pair<ValuesOverTime, PollingBasedValueProvider>(new ValuesOverTime(
                        currentSamplingInterval, statName), provider));
            if (!pollWorker.isAlive())
                pollWorker.start();
        }
    }

    public static Map<String, PlotValues<Long, Double>> getPollingSummary() {
        LinkedHashMap<String, PlotValues<Long, Double>> pollingSummary = new LinkedHashMap<String, PlotValues<Long, Double>>();
        Set<Entry<String, Pair<ValuesOverTime, PollingBasedValueProvider>>> values = pollingProviders.entrySet();
        for (Entry<String, Pair<ValuesOverTime, PollingBasedValueProvider>> v : values) {
            pollingSummary.put(v.getKey(), v.getValue().getFirst().getPlotValues());
        }
        return pollingSummary;
    }
}
