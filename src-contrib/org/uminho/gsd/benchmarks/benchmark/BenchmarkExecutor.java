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

package org.uminho.gsd.benchmarks.benchmark;


import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.interfaces.Workload.AbstractWorkloadGeneratorFactory;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class BenchmarkExecutor {

    //Node id
    private BenchmarkNodeID nodeId;

    //Benchmark interfaces
    private AbstractWorkloadGeneratorFactory workloadInterface;
    private AbstractDatabaseExecutorFactory databaseInterface;

    /**
     * number of clients on each benchmarking node  *
     */
    public int num_clients;
    /**
     * number of operations to be executed in each client *
     */
    public int num_operations;

    private List<ResultHandler> results;


    public BenchmarkExecutor(Class workloadInterface_class, String workload_conf, Class databaseInterface_class, String database_conf, int num_operations, int num_clients) {


        try {

            databaseInterface = (AbstractDatabaseExecutorFactory) databaseInterface_class.getConstructor(BenchmarkExecutor.class, String.class).newInstance(this, database_conf);
            workloadInterface = (AbstractWorkloadGeneratorFactory) workloadInterface_class.getConstructor(BenchmarkExecutor.class, String.class).newInstance(this, workload_conf);
            workloadInterface.setDatabaseFactory(databaseInterface);

        } catch (NoSuchMethodException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InvocationTargetException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InstantiationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IllegalAccessException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        this.num_operations = num_operations;
        this.num_clients = num_clients;

        results = new ArrayList<ResultHandler>();

    }


    public void prepare() throws Exception {
        workloadInterface.init();
    }

    public void run(BenchmarkNodeID id) {

        workloadInterface.setNodeId(id);
        databaseInterface.setNodeId(id);
        workloadInterface.setClientNumber(num_clients);
        databaseInterface.setClientNumber(num_clients);

        nodeId = id;
        //Synchronization Barrier

        final String workloadName = workloadInterface.getName();
        final CountDownLatch synchronizationBarrier = new CountDownLatch(num_clients);

		ResultHandler stats_handler = new ResultHandler("workloadName",-1);
		results.add(stats_handler);
		databaseInterface.setStats_handler(stats_handler);
		databaseInterface.startStats();

        for (int client_index = 0; client_index < num_clients; client_index++) {

            final DatabaseExecutorInterface executor = databaseInterface.getDatabaseClient();
            final WorkloadGeneratorInterface workloadGenerator = workloadInterface.getClient();
            final ResultHandler resultHandler = new ResultHandler(workloadName, -1);
            results.add(resultHandler);

            //Create a runnable to run the client. executor.start() must be sequential
            Runnable clientRunnable = new Runnable() {
                public void run() {
                    executor.start(workloadGenerator, nodeId, num_operations, resultHandler);
                    synchronizationBarrier.countDown();
                }
            };
            Thread clientThread = new Thread(clientRunnable, "client:" + client_index);
            clientThread.start();
        }

        try {
            synchronizationBarrier.await();
        } catch (InterruptedException e) {
            System.out.println("[ERROR:] Error in client execution. Interruption on synchronization barrier");
        }
        workloadInterface.finishExecution(results);
    }

    protected AbstractDatabaseExecutorFactory getDatabaseInterface() {
        return databaseInterface;
    }

    public void consolidate() throws Exception {
        BenchmarkSlave.terminated = true;
        workloadInterface.consolidate();
    }
}
