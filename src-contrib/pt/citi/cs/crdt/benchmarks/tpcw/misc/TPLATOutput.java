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

import swift.utils.Pair;

public class TPLATOutput {

    private static OutputStream aggregateFile, TPMFile, MeanLatencyFile;
    private static Set<String> excludedKeywords;
    private static List<File> inputFiles;
    private static Map<String, List<List<Pair<Integer, Long>>>> opStats;
    private static int tw;

    /**
     * @param args
     */
    public static void main(String[] args) {

        init();

        if (args.length == 0) {
            printUsage();
            return;
        }
        if (processArguments(args)) {
            processFiles();
            outputResults();
        }
        closeFiles();

    }

    private static void init() {
        excludedKeywords = new HashSet<String>();
        excludedKeywords.add("--help");
        excludedKeywords.add("-h");
        excludedKeywords.add("--output");
        excludedKeywords.add("-o");
        excludedKeywords.add("-input");
        excludedKeywords.add("-i");
        excludedKeywords.add("-tw");
    }

    private static void closeFiles() {
        try {
            if (aggregateFile != null)
                aggregateFile.close();
            if (TPMFile != null)
                TPMFile.close();
            if (MeanLatencyFile != null)
                MeanLatencyFile.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void outputResults() {
        try {
            printHeaders();
            int counter = 0;
            int maxSize = 0;
            while (counter >= 0) {
                for (Entry<String, List<List<Pair<Integer, Long>>>> op : opStats.entrySet()) {
                    String aggregateOutput = op.getKey() + "\t";
                    String TPMOutput = op.getKey() + "\t";
                    String LatencyOutput = op.getKey() + "\t";
                    if (op.getValue().size() > maxSize)
                        maxSize = op.getValue().size();
                    if (op.getValue().size() > counter) {
                        List<Pair<Integer, Long>> clients = op.getValue().get(counter);

                        aggregateOutput += tw * counter;
                        TPMOutput += tw * counter;
                        LatencyOutput += tw * counter;
                        double meanLatency = 0;
                        int tpm = 0;
                        for (Pair<Integer, Long> clientsi : clients) {
                            aggregateOutput += "\t" + clientsi.getFirst();
                            if (clientsi.getFirst() > 0)
                                meanLatency += (clientsi.getSecond() / (double) clientsi.getFirst())
                                        / (double) clients.size();
                            tpm += clientsi.getFirst();
                        }
                        LatencyOutput += "\t" + meanLatency;
                        TPMOutput += "\t" + tpm;
                        aggregateFile.write((aggregateOutput + "\n").getBytes());
                        TPMFile.write((TPMOutput + "\n").getBytes());
                        MeanLatencyFile.write((LatencyOutput + "\n").getBytes());
                    }
                }
                counter++;
                if (counter == maxSize)
                    counter = -1;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void printHeaders() {
        // TODO Auto-generated method stub

    }

    private static void processFiles() {
        // Map: Operation -> Count per client
        opStats = new HashMap<String, List<List<Pair<Integer, Long>>>>();
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
                while (fr.hasNextLine()) {
                    String line = fr.nextLine();
                    processLine(line, i, startTime.get(i));
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

    private static void processLine(String line, int i, long refTime) {
        String[] args = line.split("\t");
        String op_name = args[1];
        long op_startTime = Long.parseLong(args[2]);
        long op_resultTime = Long.parseLong(args[5]);
        List<List<Pair<Integer, Long>>> clientOps = opStats.get(op_name);
        int opTimeWindow;
        if (tw > 0) {
            opTimeWindow = (int) (op_startTime - refTime) / (tw * 1000);
            while (opTimeWindow >= clientOps.size()) {
                clientOps.add(initListForClients(inputFiles.size()));
            }
        } else {
            opTimeWindow = 0;
        }
        Pair<Integer, Long> currCount = clientOps.get(opTimeWindow).get(i);
        currCount.setFirst(currCount.getFirst() + 1);
        currCount.setSecond(currCount.getSecond() + op_resultTime - op_startTime);

    }

    private static void initMap(int numClients) {
        for (Operations op : Operations.values()) {
            LinkedList<List<Pair<Integer, Long>>> t0 = new LinkedList<List<Pair<Integer, Long>>>();
            t0.add(initListForClients(numClients));
            opStats.put(op.toString(), t0);

        }
    }

    private static List<Pair<Integer, Long>> initListForClients(int numClients) {
        List<Pair<Integer, Long>> clients = new LinkedList<Pair<Integer, Long>>();
        for (int i = 0; i < numClients; i++) {
            clients.add(new Pair<Integer, Long>(0, 0l));
        }
        return clients;

    }

    private static void printUsage() {
        StringBuilder builder = new StringBuilder();
        builder.append("-tw [arg]:\t\t\tThe size of the time window to aggregae the results in seconds (default: runTime)\n");
        builder.append("-h,--help:\t\t\tPrints this Help dialog\n");
        builder.append("-o [arg];--output [arg]:\tThe output filename\n");
        builder.append("-i [arg]*;--input [arg]*:\tA non-empty sequence of input files\n");
        System.out.println(builder);

    }

    private static boolean processArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                printUsage();
            }
            if (args[i].equals("-o") || args[i].equals("--output")) {
                if (i + 1 < args.length && !excludedKeywords.contains(args[i + 1])) {
                    String filename = args[++i];
                    File file = new File(filename);
                    File fileTPM = new File(filename + "_TPM");
                    File fileLat = new File(filename + "Lat");

                    try {
                        if (!file.exists())
                            if (!file.createNewFile()) {
                                System.out.println("Impossible to create file: " + file.getPath());
                            }
                        aggregateFile = new FileOutputStream(file);
                        if (!fileTPM.exists())
                            if (!fileTPM.createNewFile()) {
                                System.out.println("Impossible to create file: " + fileTPM.getPath());
                            }
                        TPMFile = new FileOutputStream(fileTPM);
                        if (!fileLat.exists())
                            if (!fileLat.createNewFile()) {
                                System.out.println("Impossible to create file: " + fileLat.getPath());
                            }
                        MeanLatencyFile = new FileOutputStream(fileLat);
                    } catch (IOException e) {
                        System.out.println("Impossible to create file: " + fileLat.getPath());
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
            if (args[i].equals("-tw")) {
                if (i + 1 < args.length && !excludedKeywords.contains(args[i + 1])) {
                    tw = Integer.parseInt(args[++i]);
                }
            }
        }
        return inputFiles.size() > 0 && aggregateFile != null;
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
}
