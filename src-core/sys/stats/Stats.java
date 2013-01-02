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
 * Collects statistics for different types of sources over time. Enables
 * collecting values opportunistically, or periodically, with different
 * semantics. Class is intended to use as a singleton, which requires special
 * attention to re-use of names.
 * 
 * @author balegas
 * 
 */
public final class Stats {

    public static final int SAMPLING_INTERVAL_MILLIS = 1000;

    private static Map<String, Stats> statisticsByName = new HashMap<String, Stats>();

    private Map<String, CounterOverTime> countigSources;
    private Map<String, HistogramOverTime> valuesFrequencySource;
    private Map<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>> pollingProviders;
    private Map<String, Pair<Integer, Long>> pollingUpdates;
    private int maxSamplingInterval;
    private boolean terminate = true;

    private Thread pollWorker;

    private String outputDir;

    private boolean overwriteDir;
    private static Logger logger = Logger.getLogger(Stats.class.getName());

    /**
     * Retrieves the statistics manager with a given name, or a new one if it
     * does not exist, using the default parameters.
     * 
     * @param name
     *            the name of the Statistics manager
     * @return a statistics manager instance
     */

    public synchronized static Stats getInstance(String name) {
        Stats stats = statisticsByName.get(name);
        if (stats == null) {
            stats = new Stats(name, true);
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
    public synchronized static Stats getInstance(String name, int samplingInterval, String outputDir,
            boolean overwriteDir) {
        logger.log(Level.WARNING, "Stats " + name + " already initialized ignoring output folder and sampling interval");
        Stats stats = statisticsByName.get(name);
        if (stats == null) {
            stats = new Stats(outputDir, overwriteDir, samplingInterval);
            stats.init(samplingInterval);
            statisticsByName.put(name, stats);
        }
        return stats;
    }

    private Stats(String outputDir, boolean overwriteDir) {
        this.outputDir = outputDir;
        this.overwriteDir = overwriteDir;
    }

    private Stats(String outputDir, boolean overwriteDir, int samplingInterval) {
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
                                System.out.println("here");
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

    /**
     * Writes the gathered statistics to the output folder since the creation of
     * the statistics manager. Probe names with ":" are split and created in
     * sub-folders accordingly
     * 
     * @throws IOException
     */
    public void outputAndDispose() throws IOException {
        terminate = true;
        dump();

    }

    /**
     * Returns a CounterSignalSource with the given name and value 0, or an
     * already existing one with the current value.
     * 
     * @param statName
     *            the name of the counter
     * @return CounterSignalSource
     */
    public CounterSignalSource getCountingSourceForStat(String statName) {
        CounterOverTime cs = null;
        synchronized (countigSources) {
            cs = countigSources.get(statName);
            if (cs == null) {
                cs = new CounterOverTime(maxSamplingInterval, statName);
                countigSources.put(statName, cs);
            } else {
                logger.log(Level.INFO, "CounterSignalSource " + statName + " already initialized");

            }
        }
        return cs;
    }

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
    public ValueSignalSource getValuesFrequencyOverTime(String statName, double... valueBins) {

        HistogramOverTime hist = null;
        synchronized (valuesFrequencySource) {
            hist = valuesFrequencySource.get(statName);
            if (hist == null) {
                Arrays.sort(valueBins);
                hist = new HistogramOverTime(StatsConstants.histogramTimeFrequency, valueBins, statName);
                valuesFrequencySource.put(statName, hist);
            } else {
                logger.log(Level.INFO, "ValueSignalSource " + statName + " already initialized ignoring value bins");
            }
        }
        return hist;
    }

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
                logger.log(Level.INFO, "PollingBasedValueProvider " + statName + " already initialized");

            }
        }
    }

    private void dump() throws IOException {
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
        }
        synchronized (valuesFrequencySource) {
            for (Entry<String, Pair<FixedRateValueOverTime, PollingBasedValueProvider>> pollingValues : pollingProviders
                    .entrySet()) {
                BufferedFileDumper statsOutput = createFile(dir, pollingValues.getKey() + "-poll");
                statsOutput.output(pollingValues.getValue().getFirst());
                statsOutput.close();
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