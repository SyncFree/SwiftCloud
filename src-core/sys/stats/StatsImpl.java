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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.utils.Pair;
import sys.stats.common.PlotValues;
import sys.stats.output.BufferedFileDumper;
import sys.stats.overtime.CounterOverTime;
import sys.stats.overtime.FixedRateValueOverTime;
import sys.stats.overtime.HistogramOverTime;
import sys.stats.sources.CounterSignalSource;
import sys.stats.sources.PollingBasedValueProvider;
import sys.stats.sources.ValueSignalSource;

/**
 * Implementations of statistics collecting model Class is intended to use as a
 * singleton, which requires special attention to re-use of names.
 * 
 * @author balegas
 * 
 */
public final class StatsImpl implements Stats {

    public static final int SAMPLING_INTERVAL_MILLIS = 1000;

    private static Map<String, StatsImpl> statisticsByName = new HashMap<String, StatsImpl>();

    private Map<String, CounterOverTime> countigSources;
    private Map<String, HistogramOverTime> valuesFrequencySource;
    private Map<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>> pollingProviders;
    private Map<String, Pair<Integer, Long>> pollingUpdates;
    private int maxSamplingInterval;
    private volatile boolean terminate = true;

    private Thread pollWorker;

    private String outputDir;

    private boolean overwriteDir;
    private static Logger logger = Logger.getLogger(StatsImpl.class.getName());

    /**
     * Retrieves the statistics manager with a given name, or a new one if it
     * does not exist, using the default parameters.
     * 
     * @param name
     *            the name of the Statistics manager
     * @return a statistics manager instance
     */

    public synchronized static StatsImpl getInstance(String name) {
        StatsImpl stats = statisticsByName.get(name);
        if (stats == null) {
            stats = new StatsImpl(name, true);
            stats.init();
            statisticsByName.put(name, stats);
        }
        return stats;
    }

    /**
     * Retrieves the statistics manager with a given name, or a new one if it
     * does not exist. Ignores parameter if already exists a Statistics manager
     * with the given name.
     * 
     * @param name
     *            the name of the Statistics manager
     * @param samplingInterval
     *            the frequency on which polling values are collected
     * @param outputDir
     *            the output directory for the statistics manager measures
     * @return a statistics manager instance
     */
    public synchronized static StatsImpl getInstance(String name, int samplingInterval, String outputDir,
            boolean overwriteDir) {
        StatsImpl stats = statisticsByName.get(name);
        if (stats == null) {
            stats = new StatsImpl(outputDir, overwriteDir, samplingInterval);
            stats.init(samplingInterval);
            statisticsByName.put(name, stats);
        } else {
            logger.log(Level.WARNING, "Stats " + name
                    + " already initialized ignoring output folder and sampling interval");
        }
        return stats;
    }

    private StatsImpl(String outputDir, boolean overwriteDir) {
        this.outputDir = outputDir;
        this.overwriteDir = overwriteDir;
    }

    private StatsImpl(String outputDir, boolean overwriteDir, int samplingInterval) {
        this.outputDir = outputDir;
        this.overwriteDir = overwriteDir;
    }

    private void init() {
        init(SAMPLING_INTERVAL_MILLIS);
    }

    private void init(int samplingInterval) {

        terminate = false;
        maxSamplingInterval = samplingInterval;
        countigSources = new HashMap<String, CounterOverTime>();
        valuesFrequencySource = new HashMap<String, HistogramOverTime>();
        pollingProviders = new LinkedHashMap<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>>();
        // First: Update interval; Second: Last update
        pollingUpdates = new HashMap<String, Pair<Integer, Long>>();

        pollWorker = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!terminate) {
                    long minDueTime = maxSamplingInterval;
                    try {
                        for (Entry<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>> p : pollingProviders
                                .entrySet()) {
                            Pair<FixedRateValueOverTime, PollingBasedValueProvider> pollStats = p.getValue();
                            Pair<Integer, Long> pollUpdate = pollingUpdates.get(p.getKey());

                            long currTime = System.currentTimeMillis();
                            long lastUpdate = pollUpdate.getSecond();
                            long frequency = pollUpdate.getFirst();

                            if (currTime - lastUpdate >= frequency) {
                                double value = pollStats.getSecond().poll();
                                pollStats.getFirst().setValue(value);
                                pollUpdate.setSecond(currTime);
                            }
                            long dueTime = frequency - (currTime - lastUpdate);
                            if (dueTime > 0 && dueTime <= minDueTime)
                                minDueTime = dueTime;

                        }
                        Thread.sleep(minDueTime);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        pollWorker.setDaemon(true);
        pollWorker.start();
    }

    @Override
    public void dump() throws IOException {
        File dir;
        if (!overwriteDir) {
            AtomicInteger suffixCounter = new AtomicInteger();
            do {
                int suf = suffixCounter.incrementAndGet();
                dir = new File(outputDir + "-" + suf);

            } while (dir.exists());
        } else {
            dir = new File(outputDir);
        }

        if (!dir.exists()) {
            boolean result = dir.mkdir();
            if (!result) {
                logger.log(Level.WARNING, outputDir
                        + " directory does not exist and it is impossible to create, cannot dump statistics");
                return;
            } else {
                logger.log(Level.INFO, outputDir + " directory was created successfully");
            }
        }

        if (!dir.isDirectory()) {
            logger.log(Level.WARNING, outputDir + " is not a directory, cannot dump statistics");
            return;
        }

        if (!dir.canWrite()) {
            logger.log(Level.WARNING, outputDir + " has no write permissions, cannot dump statistics");
            return;
        }
        synchronized (countigSources) {
            for (Entry<String, CounterOverTime> counter : countigSources.entrySet()) {
                BufferedFileDumper statsOutput = createFile(dir, counter.getKey() + "-count");
                statsOutput.output(counter.getValue());
                statsOutput.close();
            }
        }
        synchronized (valuesFrequencySource) {
            for (Entry<String, HistogramOverTime> histogram : valuesFrequencySource.entrySet()) {
                BufferedFileDumper statsOutput = createFile(dir, histogram.getKey() + "-histo");
                statsOutput.output(histogram.getValue());
                statsOutput.close();
            }
            for (Entry<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>> pollingValues : pollingProviders
                    .entrySet()) {
                BufferedFileDumper statsOutput = createFile(dir, pollingValues.getKey() + "-poll");
                statsOutput.output(pollingValues.getValue().getFirst());
                statsOutput.close();
            }
        }
    }

    @Override
    public void terminate() {
        terminate = true;
    }

    @Override
    public CounterSignalSource getCountingSourceForStat(String statName) {
        CounterOverTime cs = null;
        synchronized (countigSources) {
            cs = countigSources.get(statName);
            if (cs == null) {
                cs = new CounterOverTime(maxSamplingInterval, statName);
                countigSources.put(statName, cs);
            } else {
                logger.log(Level.FINE, "CounterSignalSource " + statName + " already initialized");

            }
        }
        return cs;
    }

    @Override
    public ValueSignalSource getValuesFrequencyOverTime(String statName, double... valueBins) {

        if (valueBins == null || valueBins.length == 0) {
            valueBins = new double[] {Double.MAX_VALUE};
        }
        HistogramOverTime hist = null;
        synchronized (valuesFrequencySource) {
            hist = valuesFrequencySource.get(statName);
            if (hist == null) {
                Arrays.sort(valueBins);
                hist = new HistogramOverTime(StatsConstants.histogramTimeFrequency, valueBins, statName);
                valuesFrequencySource.put(statName, hist);
            } else {
                logger.log(Level.FINE, "ValueSignalSource " + statName + " already initialized ignoring value bins");
            }
        }
        return hist;
    }

    @Override
    public void registerPollingBasedValueProvider(String statName, PollingBasedValueProvider provider, int frequency) {
        Pair<FixedRateValueOverTime, PollingBasedValueProvider> ps = null;
        synchronized (pollingProviders) {
            ps = pollingProviders.get(statName);
            if (ps == null) {

                Pair<FixedRateValueOverTime, PollingBasedValueProvider> p = new Pair<FixedRateValueOverTime, PollingBasedValueProvider>(
                        new FixedRateValueOverTime(maxSamplingInterval, statName), provider);
                pollingProviders.put(statName, p);
                pollingUpdates.put(statName, new Pair<Integer, Long>(frequency, System.currentTimeMillis()));
            } else {
                logger.log(Level.FINE, "PollingBasedValueProvider " + statName + " already initialized");

            }
        }
    }

    private BufferedFileDumper createFile(File dir, String key) throws FileNotFoundException {
        String[] filePath = key.split(":");
        String absolutPath = dir.getAbsolutePath();
        for (int i = 0; i < filePath.length - 1; i++) {
            absolutPath = absolutPath + "/" + filePath[i];
            dir = new File(absolutPath);
        }
        dir.mkdirs();
        String filename = absolutPath + "/" + filePath[filePath.length - 1];
        BufferedFileDumper statsOutput = new BufferedFileDumper(filename);
        statsOutput.init();
        return statsOutput;
    }

    public Map<String, PlotValues<Long, Double>> getPollingSummary() {
        Map<String, PlotValues<Long, Double>> map = new HashMap<String, PlotValues<Long, Double>>();
        for (Entry<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>> p : pollingProviders.entrySet()) {
            PlotValues<Long, Double> values = p.getValue().getFirst().getPlotValues();
            map.put(p.getKey(), values);
        }
        return map;
    }
}