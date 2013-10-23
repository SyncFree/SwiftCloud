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
package org.uminho.gsd.benchmarks.generic.workloads;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.KeeperException;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkSlave;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.helpers.ProgressBar;
import org.uminho.gsd.benchmarks.interfaces.ProbabilityDistribution;
import org.uminho.gsd.benchmarks.interfaces.Workload.AbstractWorkloadGeneratorFactory;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import pt.citi.cs.crdt.benchmarks.tpcw.database.TPCWSwiftCloudExecutorFactory;
import pt.citi.cs.crdt.benchmarks.tpcw.database.TPCW_SwiftCloud_Executor;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.TPCWNamingScheme;
import pt.citi.cs.crdt.benchmarks.tpcw.synchronization.SyncPrimitive;
import pt.citi.cs.crdt.benchmarks.tpcw.synchronization.SyncPrimitive.Barrier;

/**
 * Workload that generates operations to generate possible inconsistency in the
 * database due to the lack of ACID transactions.
 */
public class TPCWWorkloadFactory extends AbstractWorkloadGeneratorFactory {

    // The items and their stock in the database
    Map<String, Integer> items;

    // The item ids
    ArrayList<String> items_ids;

    // The probability distribution
    ProbabilityDistribution distribution;

    // The number of clients
    int client_number;

    // The authors names
    List<String> author_names;

    // The existing titles
    List<String> item_titles;

    /**
     * Out of stock level*
     */
    int out_of_stock;

    /**
     * Restock level*
     */
    int restock;

    /**
     * The workload values
     */
    Map<String, Double> workload_values;

    /**
     * ResultHandler*
     */
    ResultHandler globalResultHandler;

    /**
     * The path to store the result file*
     */
    String result_path = "";

    ProgressBar progressBar;

    /**
     * The generic construct that invokes the init() method.
     * 
     * @param workload
     *            The workload file name to be fetched from the configuration
     *            files, or other place
     */
    public TPCWWorkloadFactory(BenchmarkExecutor executor, String workload) {
        super(executor, workload);

        // loading the selected distribution from file.
        String distributionClass;
        Map<String, String> probabilityDistributionInfo;
        probabilityDistributionInfo = new TreeMap<String, String>();

        if (!info.containsKey("ProbabilityDistributions")) {
            System.out.println("[WARNING:] NO DISTRIBUTION INFO FOUND USING NORMAL DISTRIBUTION");
            distributionClass = "benchmarks.interfaces.ProbabilityDistribution.PowerLawDistribution";
            probabilityDistributionInfo.put("alpha", "0.0");
        } else {
            Map<String, String> distribution_info = info.get("ProbabilityDistributions");
            if (!distribution_info.containsKey("Distribution")) {
                distributionClass = "benchmarks.interfaces.ProbabilityDistribution.PowerLawDistribution";
                System.out.println("[WARNING:] NO DISTRIBUTION INFO FOUND USING NORMAL DISTRIBUTION");
                probabilityDistributionInfo.put("alpha", "0.0");
            } else {
                distributionClass = distribution_info.get("Distribution");
                for (String s : distribution_info.keySet()) {
                    probabilityDistributionInfo.put(s, distribution_info.get(s));
                }
            }
        }

        if (!info.containsKey("Configuration")) {
            System.out.println("[WARNING:] NO TPCW CONFIGURATION DATA");
            out_of_stock = 0;
            restock = 10;
        } else {
            Map<String, String> conf = info.get("Configuration");

            if (!conf.containsKey("resultPath")) {
                System.out.println("[INFO:] Default result path: /Result ");
                result_path = "./Results";
            } else {
                result_path = conf.get("resultPath");
            }

            if (!conf.containsKey("name")) {
                System.out.println("[INFO:] Default name : TPCW_WORKLOAD");
                workloadName = "TPCW_WORKLOAD";
            } else {
                workloadName = conf.get("name");
            }

        }

        try {
            distribution = (ProbabilityDistribution) Class.forName(distributionClass).getConstructor().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace(); // To change body of catch statement use File |
                                 // Settings | File Templates.
        } catch (IllegalAccessException e) {
            e.printStackTrace(); // To change body of catch statement use File |
                                 // Settings | File Templates.
        } catch (InvocationTargetException e) {
            e.printStackTrace(); // To change body of catch statement use File |
                                 // Settings | File Templates.
        } catch (NoSuchMethodException e) {
            e.printStackTrace(); // To change body of catch statement use File |
                                 // Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace(); // To change body of catch statement use File |
                                 // Settings | File Templates.
        }
        distribution.setInfo(probabilityDistributionInfo);

        if (!info.containsKey("Workload") && this.Workload.isEmpty()) {
            System.out.println("[WARNING:] NO TPCW WORKLOAD DATA");
        } else {
            workload_values = new TreeMap<String, Double>();
            if (info.containsKey("Workload")) {
                Map<String, String> conf = info.get("Configuration");

                for (String operation : conf.keySet()) {
                    double probability = Double.parseDouble(conf.get(operation));
                    workload_values.put(operation, probability);
                }
            } else {
                for (String operation : this.Workload.keySet()) {
                    double probability = Double.parseDouble(this.Workload.get(operation));
                    workload_values.put(operation, probability);
                }
            }
        }
    }

    @Override
    public void init() throws Exception {

        author_names = new ArrayList<String>();
        item_titles = new ArrayList<String>();

        DatabaseExecutorInterface databaseClient = this.databaseFactory.getDatabaseClient();

        if (this.databaseFactory instanceof TPCWSwiftCloudExecutorFactory) {
            ((TPCWSwiftCloudExecutorFactory) this.databaseFactory).initScoutEndpoint();
        }
        Barrier barrier1 = enterClients("BEFORE_PREFETCH", BenchmarkMain.numNodes, false);

        List<String> fields = new ArrayList<String>();
        
        
        fields.add("A_LNAME");
        Map<String, Map<String, Object>> authorNames = databaseClient.rangeQuery(TPCWNamingScheme.getAuthorTableName(),
                fields, -1);
        
        //FIXME: There is some problem when using cache, the first transaction always fails.
        authorNames = databaseClient.rangeQuery(TPCWNamingScheme.getAuthorTableName(),
                fields, -1);

        for (Map<String, Object> author_name : authorNames.values()) {
            author_names.add((String) author_name.get("A_LNAME"));
        }
        
        System.out.println("[INFO:] AUTHORS:" + authorNames.size());
        fields.clear();
        
        fields.add("I_TITLE");

        Map<String, Map<String, Object>> itemNames = databaseClient.rangeQuery(TPCWNamingScheme.getItemsTableName(),
                fields, -1);

        for (Map<String, Object> item : itemNames.values()) {
            item_titles.add((String) item.get("I_TITLE"));
        }
        System.out.println("[INFO:] ITEMS:" + item_titles.size());

        
        leaveClients("BEFORE_PREFETCH", BenchmarkMain.numNodes, barrier1);
        Barrier barrier2 = enterClients("AFTER_PREFETCH", BenchmarkMain.numNodes, true);
        this.distribution.init(item_titles.size() - 1, null);

        if (databaseClient instanceof TPCW_SwiftCloud_Executor) {
            ((TPCW_SwiftCloud_Executor) databaseClient).buildSearchIndex(itemNames.values());
            // TODO: temporary hack
            if (BenchmarkMain.custom_output_file != null)
                ((TPCW_SwiftCloud_Executor) databaseClient).setOutput(BenchmarkMain.custom_output_file);
        }
        System.out.println("Continue");
        leaveClients("AFTER_PREFETCH", BenchmarkMain.numNodes, barrier2);
        progressBar = new ProgressBar(executor.num_clients, executor.num_operations);

        globalResultHandler = new ResultHandler(workloadName, -1);
        Map<String, String> info = new LinkedHashMap<String, String>();

        info.put("Workload class:", TPCWWorkloadFactory.class.getName());
        info.put("Workload conf:", workloadName);
        info.put("Workload values:", workload_values.toString());
        info.put("Database Executor", databaseFactory.getClass().getName());
        info.put("Database engine conf:", databaseClient.getInfo().toString());
        info.put("----", "----");
        String think_time = (BenchmarkMain.distribution_factor == -1) ? "tpcw think time" : "user set: "
                + BenchmarkMain.distribution_factor;
        info.put("Think time", think_time);
        info.put("Client num", executor.num_clients + "");
        info.put("Operation num", executor.num_operations + "");
        info.put("Distribution", distribution.getName());
        info.put("Distribution conf", distribution.getInfo().toString());
        info.put("----", "----");

        GregorianCalendar date = new GregorianCalendar();
        String data_string = date.get(GregorianCalendar.YEAR) + "\\" + (date.get(GregorianCalendar.MONTH) + 1) + "\\"
                + date.get(GregorianCalendar.DAY_OF_MONTH) + " -- " + date.get(GregorianCalendar.HOUR_OF_DAY) + ":"
                + date.get(GregorianCalendar.MINUTE) + "";
        info.put("Start", data_string);

        globalResultHandler.setBechmark_info(info);
    }

    private Barrier enterClients(String groupName, int numNodes, boolean tentative) {
        if (numNodes <= 0)
            return null;

        String zooAddress = BenchmarkMain.zookeeperLocation;
        Barrier syncBarrier = new SyncPrimitive.Barrier(zooAddress, "/" + groupName, numNodes);
        try {
            syncBarrier.enter(tentative);
            System.out.println("Client entered barrier " + groupName);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return syncBarrier;
    }

    private void leaveClients(String groupName, int numNodes, Barrier syncBarrier) {
        if (numNodes <= 0) {
            return;
        }
        try {
            syncBarrier.leave();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Client left barrier " + groupName);

    }

    @Override
    public WorkloadGeneratorInterface getClient() {
        if (client_number == 0) {
            progressBar.printProcess(1500);
        }

        TPCWWorkloadGeneration client = new TPCWWorkloadGeneration(globalResultHandler, item_titles, author_names,
                workload_values, distribution, this.nodeID, client_number, progressBar);
        client_number++;
        return client;

    }

    @Override
    public void finishExecution(List<ResultHandler> collected_results) {

        int bought_qty = 0;
        int buying_actions = 0;
        int bought_carts = 0;
        int zeros = 0;

        for (ResultHandler client : collected_results) {
            globalResultHandler.addResults(client);

            HashMap<String, Object> map = client.getResulSet();
            if (!map.isEmpty()) {

                bought_qty += (Integer) client.getResulSet().get("total_bought");
                buying_actions += (Integer) client.getResulSet().get("buying_actions");
                bought_carts += (Integer) client.getResulSet().get("bought_carts");
                zeros += (Integer) client.getResulSet().get("zeros");
            }
        }

        System.out.println("[INFO:] TOTAL BOUGHT: " + bought_qty);
        System.out.println("[INFO:] BUYING ACTIONS: " + buying_actions);
        System.out.println("[INFO:] BOUGHT CARTS: " + bought_carts);
        // System.out.println("[INFO:] ZERO STOCK SELLS: " + zeros);
        System.out.println("[INFO:] WRITING RESULTS");

    }

    @Override
    public void consolidate() {

        if (!nodeID.isMaster()) {
            BenchmarkSlave.terminated = true;
        }

        GregorianCalendar date = new GregorianCalendar();
        String data_string = date.get(GregorianCalendar.YEAR) + "\\" + (date.get(GregorianCalendar.MONTH) + 1) + "\\"
                + date.get(GregorianCalendar.DAY_OF_MONTH) + " -- " + date.get(GregorianCalendar.HOUR_OF_DAY) + ":"
                + date.get(GregorianCalendar.MINUTE) + "";
        globalResultHandler.getBechmark_info().put("End", data_string);

        globalResultHandler.listDataToSOutput();
        globalResultHandler.listDatatoFiles(result_path, "", true);
        System.exit(0);
    }

}
