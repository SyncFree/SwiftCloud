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

package org.uminho.gsd.benchmarks.interfaces.Workload;


import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.helpers.JsonUtil;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Workload generation class. <br>
 * This class the implements this interface will pass for three phases.  <br>
 * 1.init - The class prepares execution in all benchmarking nodes at the same time <br>
 * 2.deploy the clients - The class deploys several clients that should generate instruction to be executed by a {@linkplain org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface database executor}.   <br>
 * 3.finish - after all clients, in all nodes have finished, the consolidate method is called.  <br>
 */
public abstract class AbstractWorkloadGeneratorFactory {

    protected AbstractDatabaseExecutorFactory databaseFactory;
    protected BenchmarkNodeID nodeID;
    protected String workloadName;
    private String workloadFileName;
    protected Map<String, String> Workload;
    protected Map<String, Map<String, String>> info;
    /**
     * Client number in one node*
     */
    protected int client_number;


    protected BenchmarkExecutor executor;

    /**
     * The generic construct that loads workload info.
     *
     * @param executor     The benchmark executor with info about the client number among others.
     * @param workloadFile The workload file name to be fetched from the configuration files, or other place
     */
    public AbstractWorkloadGeneratorFactory(BenchmarkExecutor executor, String workloadFile) {
        this.workloadName = workloadName;
        this.workloadFileName = workloadFile;
        this.Workload = new TreeMap<String, String>();
        this.executor = executor;
        this.info = new TreeMap<String, Map<String, String>>();
        loadFile();
    }


    /**
     * Method that loads workload info.
     */
    private void loadFile() {

        FileInputStream in = null;
        String jsonString_r = "";

        if (!workloadFileName.endsWith(".json")) {
            workloadFileName = workloadFileName + ".json";
        }

        try {
            in = new FileInputStream("conf/Workload/" + workloadFileName);
            BufferedReader bin = new BufferedReader(new InputStreamReader(in));
            String s = "";
            StringBuilder sb = new StringBuilder();
            while (s != null) {
                sb.append(s);
                s = bin.readLine();
            }
            jsonString_r = sb.toString().replace("\n", "");
            bin.close();
            in.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(AbstractWorkloadGeneratorFactory.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AbstractWorkloadGeneratorFactory.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(AbstractWorkloadGeneratorFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        Map<String, Map<String, String>> map = JsonUtil.getStringMapMapFromJsonString(jsonString_r);

        //the percentage in each the operation should be executed.
        if (map.containsKey("Workload")) {
            Workload = map.get("Workload");
            map.remove("Workload");
        } else {
            System.out.println("[WARNING:] NO REAL WORKLOAD INFO LOADED, YOU SHOULD USE THE Workload TAG");
        }

        if (!map.isEmpty()) {
            info = map;
        }

    }

    /**
     * Starts the Workload generation factory. <br>
     * This operation is synchronized between the master and slaves, and
     * should contain operations that prepare the future clients to start from a specif point
     */
    public abstract void init() throws Exception;


    /**
     * Gets a generation client. <br>
     * This method is used by the platform executor to assign a generation client to each database executor client.
     *
     * @return a workload generator.
     */
    public abstract WorkloadGeneratorInterface getClient();


    /**
     * Operations over the results <br>
     * This method is invoked after all clients finish and it is intended to allow the user to execute operations over
     * collected data.
     */
    public abstract void consolidate() throws Exception;

    /**
     * Method called after all clients have finished
     *
     * @param collected_results the results collected under this benchmark node.
     */
    public abstract void finishExecution(List<ResultHandler> collected_results);

    public void setDatabaseFactory(AbstractDatabaseExecutorFactory databaseFactory) {
        this.databaseFactory = databaseFactory;
    }

    public void setNodeId(BenchmarkNodeID id) {
        nodeID = id;
    }

    /**
     * Get workload name.<br>
     * <p/>
     * This will be used to name the log files.
     *
     * @return The workload name.
     */

    public String getName() {
        return workloadName;
    }

    /**
     * Sets the number of used clients in one node.
     */
    public void setClientNumber(int client_number) {
        this.client_number = client_number;

    }

}
