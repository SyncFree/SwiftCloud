/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.stats;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.PollingBasedValueProvider;
import sys.stats.statisticsOverTime.ConstantRateValueOverTime;
import sys.stats.statisticsOverTime.CounterOverTime;
import sys.stats.statisticsOverTime.HistogramOverTime;
import sys.stats.statisticsOverTime.ValueOverTime;

public class Stats {
    public static final int SAMPLING_INTERVAL_MILLIS = 10000;

    private static Map<String, CounterOverTime> countigSources;
    // private static Map<String, ValuesOverTime> valuesSources;
    private static Map<String, HistogramOverTime> valuesFrequencySource;
    private static Map<String, Pair<ConstantRateValueOverTime, PollingBasedValueProvider>> pollingProviders;
    private static int currentSamplingInterval;
    private static boolean terminate = true;

    private static Thread pollWorker;

    public static void init() {
        init(SAMPLING_INTERVAL_MILLIS);
    }

    public static void init(int samplingInterval) {
        if(terminate == false){
            System.out.println("already initialized");
        }
        terminate = false;
        currentSamplingInterval = samplingInterval;
        countigSources = new HashMap<String, CounterOverTime>();
        // valuesSources = new HashMap<String, ValuesOverTime>();
        valuesFrequencySource = new HashMap<String, HistogramOverTime>();
        pollingProviders = new LinkedHashMap<String, Pair<ConstantRateValueOverTime, PollingBasedValueProvider>>();

        pollWorker = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!terminate) {
                    try {
                        for (Entry<String, Pair<ConstantRateValueOverTime, PollingBasedValueProvider>> p : pollingProviders
                                .entrySet()) {
                            Pair<ConstantRateValueOverTime, PollingBasedValueProvider> pollStats = p.getValue();
                            double value = pollStats.getSecond().poll();
                            pollStats.getFirst().setValue(value);
                        }
                        Thread.sleep(currentSamplingInterval);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        pollWorker.setDaemon(true);
        pollWorker.start();
    }

    public static void dispose() {
        terminate = true;
    }

    public static CounterSignalSource getCountingSourceForStat(String statName) {
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
        Pair<ConstantRateValueOverTime, PollingBasedValueProvider> ps = null;
        synchronized (pollingProviders) {
            ps = pollingProviders.get(statName);
            if (ps == null)
                pollingProviders.put(statName, new Pair<ConstantRateValueOverTime, PollingBasedValueProvider>(
                        new ConstantRateValueOverTime(currentSamplingInterval, statName), provider));

        }
    }

    public static Map<String, PlotValues<Long, Double>> getPollingSummary() {
        LinkedHashMap<String, PlotValues<Long, Double>> pollingSummary = new LinkedHashMap<String, PlotValues<Long, Double>>();
        Set<Entry<String, Pair<ConstantRateValueOverTime, PollingBasedValueProvider>>> values = pollingProviders
                .entrySet();
        for (Entry<String, Pair<ConstantRateValueOverTime, PollingBasedValueProvider>> v : values) {
            pollingSummary.put(v.getKey(), v.getValue().getFirst().getPlotValues());
        }
        return pollingSummary;
    }
}
