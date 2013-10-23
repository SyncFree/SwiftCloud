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
package pt.citi.cs.crdt.benchmarks.tpcw.database;

import static sys.net.api.Networking.Networking;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.generic.helpers.NodeKeyGenerator;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.helpers.ThinkTime;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import pt.citi.cs.crdt.benchmarks.tpcw.synchronization.TPCWRpc;
import pt.citi.cs.crdt.benchmarks.tpcw.synchronization.TPCWRpcHandler;
import swift.utils.Pair;
import sys.Sys;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcHandle;

/**
 * TPC-W execution factory interface for Cassandra It loads the configuration
 * for Cassandra and returns execution clients.
 */
public class TPCWSwiftCloudExecutorFactory extends AbstractDatabaseExecutorFactory {

    /**
     * KeySpace name*
     */
    protected String Keyspace;
    // Database nodes connection info
    private Map<String, Integer> connections = new TreeMap<String, Integer>();

    protected String cachePolicy, isolationLevel;
    /**
     * Think time*
     */
    private int simulatedDelay;
    /**
     * The number of keys to fetch from the database in each iteration*
     */
    private int search_slice_ratio;

    NodeKeyGenerator keyGenerator;

    private Map<String, String> key_associations;
    private static ConcurrentLinkedQueue<Pair<RpcHandle, TPCWRpc>> requestsQueue;

    private static int SERVER_PORT = 8777;

    public TPCWSwiftCloudExecutorFactory(BenchmarkExecutor executor, String conf_file) {
        super(executor, conf_file);
        System.out.println(conf_file);
        init();
    }

    private void init() {

        Sys.init();
        if (!conf.containsKey("ConsistencyLevels")) {
            System.out.println("WARNING: Replication values not found");
        } else {
            Map<String, String> CL = (Map<String, String>) conf.get("ConsistencyLevels");
            isolationLevel = CL.get("ISOLATION_LEVEL");
            cachePolicy = CL.get("CACHE_POLICY");

        }

        // if (!conf.containsKey("ScoutsMapping")) {
        // System.out.println("WARNING: Replication values not found");
        // } else {
        // ScoutsMapping = new HashMap<String, Pair<String, ScoutProto>>();
        // Map<String, String> CL = (Map<String, String>) conf
        // .get("ScoutsMapping");
        // for (Entry<String, String> entry : CL.entrySet()) {
        // ScoutsMapping.put(entry.getKey(), new Pair<String, ScoutProto>(
        // entry.getValue(), null));
        // }
        //
        // }

        Keyspace = "Tpcw";

        if (!conf.containsKey("DataBaseInfo")) {
            System.out.println("ERROR: NO DATABASE INFO FOUND DEFAULTS ASSUMED: KEYSPACE=Tpcw");
        } else {
            Map<String, String> CI = (Map<String, String>) conf.get("DataBaseInfo");
            Keyspace = CI.get("keyspace");
        }

        if (!conf.containsKey("DataBaseConnections")) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=8087] ");
            connections.put("localhost", 8087);
        } else {
            Map<String, String> CI = (Map<String, String>) conf.get("DataBaseConnections");
            for (String host : CI.keySet()) {
                int port = Integer.parseInt(CI.get(host).trim());
                connections.put(host, port);
                System.out.println("SwiftCloud native database client registered: " + host + ":" + port);
            }
        }
        if (connections.isEmpty()) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=8087] ");
            connections.put("localhost", 8087);
        }

        if (!conf.containsKey("ColumnPaths")) {
            System.out.println("WARNING: KEY ASSOCIATIONS NOT FOUND");
            key_associations = new TreeMap<String, String>();
        } else {
            key_associations = conf.get("ColumnPaths");
        }

        if (!conf.containsKey("Configuration")) {
            System.out
                    .println("[WARN:] RETRIEVED SLICES -> 1000 rows, add \"Configuration\" section and a \"retrievedRowSlices\" parameter");
            search_slice_ratio = 1000;
        } else {
            Map<String, String> CI = conf.get("Configuration");

            if (CI.containsKey("retrievedRowSlices")) {
                search_slice_ratio = Integer.parseInt(CI.get("retrievedRowSlices"));
            } else {
                System.out
                        .println("[ERROR:] NO CONFIGURATION FOUND: RETRIEVED SLICES -> 1000 rows, add a \"retrievedRowSlices\" parameter to the \"Configuration\" section");
                search_slice_ratio = 1000;

            }
        }
        if (connections.isEmpty()) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
            connections.put("localhost", 8098);
        }

        System.out.println("Think Time Sample: " + ThinkTime.getThinkTime() + "," + ThinkTime.getThinkTime());

        initTPMCounting();

    }

    @Override
    public DatabaseExecutorInterface getDatabaseClient() {

        if (keyGenerator == null && nodeID != null) {
            keyGenerator = new NodeKeyGenerator(this.nodeID.getId());
        }

        // if (ScoutsMapping.containsKey(BenchmarkMain.swiftCloudNodeID)) {
        // Pair<String, ScoutProto> pair = ScoutsMapping
        // .get(BenchmarkMain.swiftCloudNodeID);
        // if (pair.getSecond() == null) {
        // File outputFile = new File(conf.get("ScoutsResultPath").get(
        // "path")+"/scoutResults");
        // String[] connection = pair.getFirst().split(":");
        // try {
        // scout = new ScoutProto(connection[0],
        // Integer.parseInt(connection[1]),
        // new FileOutputStream(outputFile));
        // } catch (NumberFormatException e) {
        // e.printStackTrace();
        // } catch (FileNotFoundException e) {
        // e.printStackTrace();
        // }
        // pair.setSecond(scout);
        // scout.init(BenchmarkMain.workload_alias,
        // BenchmarkMain.swiftCloudNodeID, BenchmarkMain.numNodes,
        // BenchmarkMain.number_threads, BenchmarkMain.thinkTime,
        // BenchmarkMain.distribution_factor);
        // } else
        // scout = pair.getSecond();
        // }

        TPM_counter tpm_counter = new TPM_counter();
        registerCounter(tpm_counter);

        initScoutEndpoint();

        return new TPCW_SwiftCloud_Executor(Keyspace, connections, key_associations, simulatedDelay,
                search_slice_ratio, keyGenerator, tpm_counter, isolationLevel, cachePolicy, 
                requestsQueue);

    }

    public void initScoutEndpoint() {
        if (requestsQueue == null) {
            requestsQueue = new ConcurrentLinkedQueue<Pair<RpcHandle, TPCWRpc>>();

            new Thread(new Runnable() {

                @Override
                public void run() {
                    Networking.rpcBind(SERVER_PORT, TransportProvider.DEFAULT).toService(0, new TPCWRpcHandler() {
                        @Override
                        public void onReceive(final RpcHandle handle, final TPCWRpc msg) {
                            msg.setReceivedTime(System.currentTimeMillis());
                            Pair<RpcHandle, TPCWRpc> pair = new Pair<RpcHandle, TPCWRpc>(handle, msg);
                            // try {
                            // Thread.sleep(50);
                            // } catch (InterruptedException e) {
                            // e.printStackTrace();
                            // }
                            
                            requestsQueue.add(pair);
                        }
                    });
                }
            }).start();
        }
    }

}