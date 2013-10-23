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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uminho.gsd.benchmarks.dataStatistics;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentResultHandler {

    private int testsamples = 100;
    String name;
    private ConcurrentHashMap<String, ArrayList<Long>> results;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> events;

    /**
     * @param run_data_divisions the devision in the data by runs. If -1 all will be inserted un the same run;
     */
    public ConcurrentResultHandler(String name, int run_data_divisions) {
        this.name = name;
        testsamples = run_data_divisions;
        results = new ConcurrentHashMap<String, ArrayList<Long>>();
        events = new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();
    }

    public synchronized void logResult(String operation, long result) {

        if (!results.containsKey(operation)) {
            results.put(operation, new ArrayList<Long>());
        }
        results.get(operation).add(result);
    }

    public synchronized void countEvent(String eventType, String event, long number) {

        if (!events.containsKey(eventType)) {
            ConcurrentHashMap<String, Long> new_events = new ConcurrentHashMap<String, Long>();
            new_events.put(event, number);
            events.put(eventType, new_events);
        } else {
            if (!events.get(eventType).containsKey(event)) {
                events.get(eventType).put(event, number);
            } else {
                long count = events.get(eventType).get(event) + number;
                events.get(eventType).put(event, count);
            }
        }

    }

    public void cleanResults() {
        results.clear();
    }

    public void addResults(ConcurrentResultHandler other_results) {

        ConcurrentHashMap<String, ArrayList<Long>> new_results = other_results.results;

        for (String event_name : new_results.keySet()) {
            if (!this.results.containsKey(event_name)) {
                this.results.put(event_name, new_results.get(event_name));
            } else {
                for (Long l : new_results.get(event_name)) {
                    this.results.get(event_name).add(l);
                }
            }
        }

        ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> new_events = other_results.events;

        for (String event_name : new_events.keySet()) {
            if (!this.events.containsKey(event_name)) {
                this.events.put(event_name, new_events.get(event_name));
            } else {
                ConcurrentHashMap<String, Long> new_event_count = new_events.get(event_name);
                ConcurrentHashMap<String, Long> this_event_count = this.events.get(event_name);
                for (String event_count_name : new_event_count.keySet()) {
                    if (this_event_count.containsKey(event_count_name)) {
                        this_event_count.put(event_count_name, this_event_count.get(event_count_name) + new_event_count.get(event_count_name));
                    } else {
                        this_event_count.put(event_count_name, new_event_count.get(event_count_name));
                    }
                }
            }
        }

    }

    public void listDataToSOutput() {

        System.out.println("\n\n------- RESULTS FOR: " + name + "-------");
        System.out.println("--runs: " + testsamples);
        for (String dataOperation : results.keySet()) {
            System.out.println("OPERATION: " + dataOperation);
            ArrayList<Long> result_data = results.get(dataOperation);
            boolean do_multipleruns = testsamples >= 0;


            int total_amount = 0;
            int currrent_amount = 0;
            int current_run = 0;
            int run = 0;
            ArrayList<Long> run_result = new ArrayList<Long>();
            for (Long res : result_data) {

                run_result.add(res);
                total_amount += res;
                currrent_amount += res;
                current_run += 1;

                if (do_multipleruns && current_run == testsamples) {
                    System.out.println("--RESULTS FOR RUN " + run + "");
                    double average = (currrent_amount * 1.0d) / (testsamples * 1.0d);
                    System.out.println("Average: " + average);
                    double variance = 0.0;
                    long aux = 0;
                    for (Long run_res : run_result) {
                        aux += Math.pow((run_res - average), 2);
                    }
                    variance = aux * (1d / (current_run - 1d));
                    System.out.println("Variance: " + variance);

                    run++;
                    currrent_amount = 0;
                    current_run = 0;


                    run_result = new ArrayList<Long>();
                }
            }
            System.out.println("----TOTAL RESULTS:----");
            double average = (total_amount * 1.0d) / (testsamples * 1.0d);
            System.out.println("Average: " + average);
            double variance = 0.0;
            long aux = 0;
            for (Long run_res : result_data) {
                aux += Math.pow((run_res - average), 2);
            }
            variance = aux * (1d / (current_run - 1d));
            System.out.println("Variance: " + variance);

        }
        System.out.println("****EVENT COUNT****");
        for (String eventType : events.keySet()) {
            System.out.println("+EVENT TYPE: " + eventType);
            for (String event : events.get(eventType).keySet()) {
                System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
            }
        }
    }

    public void listDataToFile(String filename) {
    }

    public void listDataToFile(File filename) {
    }

    public void listDatatoFiles(String filePerfix) {
    }

    public void doRstatistcs(String filePerfix) {
    }
}
