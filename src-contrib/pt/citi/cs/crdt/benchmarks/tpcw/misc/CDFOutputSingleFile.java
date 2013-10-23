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
package pt.citi.cs.crdt.benchmarks.tpcw.misc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

public class CDFOutputSingleFile {

    private static final int DEFAULT_BIN_SIZE = 10;
    private static final int DEFAULT_MAX_OP_DURATION = 2500;
    private static final int LINES_TO_DISCARD = 200;
    private static final int LINES_TO_PROCESS = 1500;
    private static OutputStream CDFFile;
    private static Set<String> excludedKeywords;
    private static List<File> inputFiles;
    private static Map<String, List<int[]>> opStats;
    private static int bs;
    private static Map<String, List<Integer>> opCount;
    private static long opMaxLatency;

    private static void printUsage() {
        StringBuilder builder = new StringBuilder();
        builder.append("-bs [arg]:\t\t\tThe bi size to aggregate results (ms)");
        builder.append("-h,--help:\t\t\tPrints this Help dialog\n");
        builder.append("-o [arg];--output [arg]:\tThe output filename\n");
        builder.append("-i [arg]*;--input [arg]*:\tA non-empty sequence of input files\n");
        System.out.println(builder);

    }

    private static boolean processArguments(String[] args) {

        bs = DEFAULT_BIN_SIZE;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                printUsage();
            }
            if (args[i].equals("-o") || args[i].equals("--output")) {
                if (i + 1 < args.length && !excludedKeywords.contains(args[i + 1])) {
                    String filename = args[++i];
                    File file = new File(filename);

                    try {
                        if (!file.exists())
                            if (!file.createNewFile()) {
                                System.out.println("Impossible to create file: " + file.getPath());
                            }
                        CDFFile = new FileOutputStream(file);
                    } catch (IOException e) {
                        System.out.println("Impossible to create file: " + file.getPath());
                    }
                }
            }
            if (args[i].equals("-i") || args[i].equals("--input")) {
                inputFiles = new LinkedList<File>();
                while (i + 1 < args.length && !excludedKeywords.contains(args[i + 1])) {
                    String token = args[++i];
                    File file = new File(token);
                    if (file.exists() && checkHeader(file))
                        inputFiles.add(file);
                }
                if (inputFiles.size() == 0) {
                    System.out.println("No valid input file was added");
                }

            }
            if (args[i].equals("-bs")) {
                if (i + 1 < args.length && !excludedKeywords.contains(args[i + 1])) {
                    bs = Integer.parseInt(args[++i]);
                }
            }
        }

        return inputFiles.size() > 0 && CDFFile != null;
    }

    private static boolean checkHeader(File file) {
        try {
            Scanner fr = new Scanner(file);
            fr.nextLine();
            String line = fr.nextLine();
            if (line.equals("THREAD_ID\tOPERATION\tSTART_TIME\tOP_RECEIVED_TIME\tOP_EXECUTION_END_TIME\tTOTAL_TIME")) {
                fr.close();
                return true;
            }
            fr.close();
            return false;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    public static void main(String[] args) {

        init();

        if (args.length == 0) {
            printUsage();
            return;
        }
        if (processArguments(args)) {
            processFiles();
            dumpCount();
            outputResults();
        }
        closeFiles();

    }

    private static void dumpCount() {
        for (Entry<String, List<int[]>> op : opStats.entrySet()) {
            System.out.println(op.getKey());
            int runCount = 1;
            for (int[] probs : op.getValue()) {
                System.out.print("RUN " + runCount + " ");
                for (int i = 0; i < probs.length; i++) {
                    System.out.print(probs[i] + " ");
                }
                runCount++;
                System.out.println();
            }
            System.out.println();
        }
    }

    private static void initMap(int numRuns) {
        opStats = new HashMap<String, List<int[]>>();
        opCount = new HashMap<String, List<Integer>>();
        for (Operations op : Operations.values()) {
            List<int[]> runs = new LinkedList<int[]>();
            List<Integer> opCounts = new LinkedList<Integer>();
            for (int i = 0; i < numRuns; i++) {
                runs.add(new int[DEFAULT_MAX_OP_DURATION / bs]);
                opCounts.add(0);
            }
            opStats.put(op.toString(), runs);
            opCount.put(op.toString(), opCounts);
        }
    }

    // Computes an array with the frequencies of of each operation's latency
    private static void processFiles() {
        // Map: Operation -> Count per client
        initMap(inputFiles.size());
        List<Scanner> frList = new LinkedList<Scanner>();
        List<Long> startTime = new LinkedList<Long>();
        try {
            for (File file : inputFiles) {
                Scanner scanner = new Scanner(file);
                frList.add(scanner);
                scanner.next();
                long startTimeL = scanner.nextLong();
                startTime.add(startTimeL);
                scanner.nextLine();
                scanner.nextLine();
            }

            for (int i = 0; i < frList.size(); i++) {
                Scanner fr = frList.get(i);
                for (int j = 0; j < LINES_TO_DISCARD; j++) {
                    fr.nextLine();
                }
                int count = 0;
                while (fr.hasNextLine() && count < LINES_TO_PROCESS) {
                    String line = fr.nextLine();
                    processLine(line, i, startTime.get(i));
                    count++;
                }
            }
            for (Scanner fr : frList) {
                fr.close();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void processLine(String line, int run, long refTime) {
        String[] args = line.split("\t");
        String op_name = args[1];
        long op_startTime = Long.parseLong(args[2]);
        long op_resultTime = Long.parseLong(args[5]);
        int[] clientOps = opStats.get(op_name).get(run);
        long duration = op_resultTime - op_startTime;
        if (duration > opMaxLatency) {
            opMaxLatency = duration;
        }
        int binIndex = (int) duration / bs;
        if (binIndex > clientOps.length) {
            System.out.println("WARINING OPERATION NOT COUNTED DUE TO INSUFFICIENT BINS");
        } else {
            clientOps[binIndex] = clientOps[binIndex] + 1;
            opCount.get(op_name).set(run, opCount.get(op_name).get(run) + 1);
        }

    }

    private static void printHeaders() {
        try {
            CDFFile.write("OP\t".getBytes());
            for (int i = 0; i <= DEFAULT_MAX_OP_DURATION / bs; i++) {
                CDFFile.write(("LP" + (i * bs) + "\t").getBytes());
            }
            CDFFile.write("\n".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void outputResults() {
        try {
            printHeaders();
            int numRuns = inputFiles.size();
            for (Entry<String, List<int[]>> op : opStats.entrySet()) {
                CDFFile.write((op.getKey() + "\t").getBytes());
                double probCum = 0.0;
                for (int j = 0; j < DEFAULT_MAX_OP_DURATION / bs; j++) {
                    for (int i = 0; i < op.getValue().size(); i++) {
                        probCum += (op.getValue().get(i)[j] / (double) opCount.get(op.getKey()).get(i))
                                / (double) numRuns;
                    }
                    CDFFile.write((probCum + "\t").getBytes());
                }
                CDFFile.write(("\n").getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void init() {
        excludedKeywords = new HashSet<String>();
        excludedKeywords.add("--help");
        excludedKeywords.add("-h");
        excludedKeywords.add("--output");
        excludedKeywords.add("-o");
        excludedKeywords.add("-input");
        excludedKeywords.add("-i");
        excludedKeywords.add("-bs");
    }

    private static void closeFiles() {
        try {
            if (CDFFile != null)
                CDFFile.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
