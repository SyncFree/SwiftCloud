/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.probabilityDistributions;

import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.interfaces.ProbabilityDistribution;

import java.io.*;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZipfDistribution implements ProbabilityDistribution {

    private Random rnd = new Random(System.currentTimeMillis());
    private int size;
    private double skew;
    private double bottom = 0;
    Map<String, String> info;

    public ZipfDistribution() {

    }

    public ZipfDistribution(int numberElements, double skew) {
        this.size = numberElements;
        this.skew = skew;

        for (int i = 1; i < size; i++) {
            this.bottom += (1 / Math.pow(i, this.skew));
        }

    }


    public String getName() {
        return "Zipf distribution";  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Map<String, String> getInfo() {
        if(info==null)
            return new TreeMap<String, String>();

        if(BenchmarkMain.distribution_factor!=-1){
           info.put("skew",this.skew+"");
        }

        return info;
    }

    public void init(int numberElements, Map<String, Object> options) {
        this.size = numberElements;
        this.skew = 0.5;

        if(BenchmarkMain.distribution_factor!=-1){
            System.out.println("Power law factor set to user defined level: " +BenchmarkMain.distribution_factor);
            skew = BenchmarkMain.distribution_factor;
            return;
        }

        if (info == null || !info.containsKey("skew")) {
            System.out.println("[WARN:] SKEW OPTION IS NOT DEFINED IN USED ZIPF DISTRIBUTION. DEFAULT: 0.5");
        } else {
            this.skew = Double.parseDouble(info.get("skew").trim());
        }
        for (int i = 1; i < size; i++) {
            this.bottom += (1 / Math.pow(i, this.skew));
        }
    }


    public void setInfo(Map<String, String> info) {
        this.info = info;
    }

    // the next() method returns an rank id. The frequency of returned rank ids are follows ZipfDistribution distribution.
    public int getNextElement() {

        int rank;
        double frequency = 0;
        double dice;

        rank = rnd.nextInt(size);
        frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();

        while (!(dice < frequency)) {
            rank = rnd.nextInt(size);
            frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }

        return rank;
    }

    public ProbabilityDistribution getNewInstance() {
        return new ZipfDistribution(this.size, this.skew);
    }

    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }


    public static void main(String[] args) {
        TreeMap<Integer, Integer> ints = new TreeMap<Integer, Integer>();


        for (int z = 0; z < 1000; z++) {
            ints.put(z, 0);
        }

        ProbabilityDistribution dis = new ZipfDistribution(1000, 1.5);
        for (int i = 0; i < 10000; i++) {
            int num = dis.getNextElement();
            int o = ints.get(num) + 1;
            ints.put(num, o);
        }

        for (int no : ints.keySet()) {
            System.out.println(no + "  : " + ((ints.get(no) * 100d) / 10000d) + "% ");


        }

        File event_results_file = new File("/Users/pedro/Desktop/DistributionResults/Untitled.txt");
        FileOutputStream out = null;
        BufferedOutputStream stream = null;
        try {
            out = new FileOutputStream(event_results_file);
            stream = new BufferedOutputStream(out);

        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        try {
            out.write("index , occurrence\n".getBytes());
            for (int no : ints.keySet()) {

                // System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));

                out.write((no + "  , " + ((ints.get(no) * 100d) / 10000d) + "\n").getBytes());

            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        try {
            stream.flush();
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(ZipfDistribution.class.getName()).log(Level.SEVERE, null, ex);
            }


        }


    }


}
       


