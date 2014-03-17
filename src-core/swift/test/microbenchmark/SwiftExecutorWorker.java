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
package swift.test.microbenchmark;

import java.util.Random;

import swift.client.AbstractObjectUpdatesListener;
import swift.crdt.IntegerCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.crdt.core.CRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.test.microbenchmark.interfaces.MicroBenchmarkWorker;
import swift.test.microbenchmark.interfaces.ResultHandler;
import swift.test.microbenchmark.interfaces.WorkerManager;

public class SwiftExecutorWorker implements MicroBenchmarkWorker {

    private WorkerManager manager;
    private SwiftSession clientServer;
    private CRDTIdentifier[] identifiers;
    private double updateRatio;
    private String workerID;
    private int maxTxSize;
    private Random random;
    private boolean stop;

    protected long startTime, endTime;
    protected int numExecutedTransactions, writeOps, readOps;
    private RawDataCollector rawData;
    private CachePolicy cachePolicy;
    private IsolationLevel isolationLevel;
    private MicroBenchmarkUpdateListener listener;

    public SwiftExecutorWorker(WorkerManager manager, String workerID, CRDTIdentifier[] identifiers,
            double updateRatio, Random random, SwiftSession clientServer, int maxTxSize, CachePolicy cachePolicy,
            IsolationLevel isolationLevel, int runCount, String outputDir) {
        this.manager = manager;
        this.identifiers = identifiers;
        this.updateRatio = updateRatio;
        this.workerID = workerID;
        this.random = random;
        this.clientServer = clientServer;
        this.maxTxSize = maxTxSize;
        this.rawData = manager.getNewRawDataCollector(workerID, runCount, outputDir);
        this.cachePolicy = cachePolicy;
        this.isolationLevel = isolationLevel;
        this.listener = new MicroBenchmarkUpdateListener();

    }

    @Override
    public void run() {
        manager.onWorkerStart(this);
        startTime = System.currentTimeMillis();
        while (!stop) {
            try {
                OpType operationType = (random.nextDouble() > updateRatio) ? OpType.READ_ONLY : OpType.UPDATE;

                switch (operationType) {

                case UPDATE: {
                    long txStartTime = System.nanoTime();
                    TxnHandle txh = clientServer.beginTxn(isolationLevel, cachePolicy, false);
                    int randomIndex = random.nextInt(identifiers.length);// (int)
                                                                         // Math.floor(random.nextDouble()
                                                                         // *
                                                                         // identifiers.length);
                    IntegerCRDT integerCRDT = txh.get(identifiers[randomIndex], false, IntegerCRDT.class,
                            listener);
                    if (random.nextDouble() > 0.5) {
                        integerCRDT.add(10);
                    } else {
                        integerCRDT.sub(10);
                    }
                    txh.commit();
                    long txEndTime = System.nanoTime();
                    rawData.registerOperation(txEndTime - txStartTime, 1, 1, txStartTime);
                    writeOps++;
                    break;
                }
                case READ_ONLY: {
                    long txStartTime = System.nanoTime();
                    int txSize = (int) Math.ceil(random.nextDouble() * maxTxSize);
                    TxnHandle txh = clientServer.beginTxn(isolationLevel, cachePolicy, true);
                    for (int i = 0; i < txSize; i++) {
                        int randomIndex = (int) Math.floor(Math.random() * identifiers.length);
                        txh.get(identifiers[randomIndex], false, IntegerCRDT.class, listener);
                        readOps++;
                    }
                    txh.commit();
                    long txEndTime = System.nanoTime();
                    rawData.registerOperation(txEndTime - txStartTime, 0, txSize, txStartTime);
                    break;
                }
                default:
                    break;
                }
                numExecutedTransactions++;

            } catch (NetworkException e) {
                e.printStackTrace();
            } catch (WrongTypeException e) {
                e.printStackTrace();
            } catch (NoSuchObjectException e) {
                e.printStackTrace();
            } catch (VersionNotFoundException e) {
                e.printStackTrace();
            }
        }
        // System.out.println("STOP CLIENT");
        clientServer.stopScout(true);
        endTime = System.currentTimeMillis();
        manager.onWorkerFinish(this);

    }

    @Override
    public ResultHandler getResults() {
        return new SwiftOperationExecutorResultHandler(this);
    }

    @Override
    public void stop() {
        stop = true;

    }

    @Override
    public String getWorkerID() {
        return workerID;
    }

    @Override
    public RawDataCollector getRawData() {
        return rawData;
    }

    class MicroBenchmarkUpdateListener extends AbstractObjectUpdatesListener {

        @Override
        public void onObjectUpdate(TxnHandle txn_old, CRDTIdentifier id, CRDT<?> previousValue) {
            // System.out.println("Object Modified " + id);

        }
    }

}

class SwiftOperationExecutorResultHandler implements ResultHandler {

    private double executionTime;
    private String workerID;
    private int numExecutedTransactions, writeOps, readOps;
    private RawDataCollector rawData;

    public SwiftOperationExecutorResultHandler(SwiftExecutorWorker worker) {
        executionTime = (worker.endTime - worker.startTime);
        workerID = worker.getWorkerID();
        readOps = worker.readOps;
        writeOps = worker.writeOps;
        numExecutedTransactions = (int) worker.numExecutedTransactions;
        rawData = worker.getRawData();
    }

    @Override
    public String toString() {
        String results = workerID + " Results:\n";
        results += "Execution Time:\t" + executionTime + "ms" + "\n";
        results += "Executed Transactions:\t" + numExecutedTransactions + " W:\t" + writeOps + "\tR:\t" + readOps
                + "\n";
        results += "Throughput(Tx/min):\t" + numExecutedTransactions / ((executionTime) / (1000 * 60d)) + "\n";
        results += "Throughput(Tx/sec):\t" + numExecutedTransactions / ((executionTime) / (1000)) + "\n";
        return results;
    }

    @Override
    public double getExecutionTime() {
        return executionTime;
    }

    @Override
    public int getNumExecutedTransactions() {
        return numExecutedTransactions;
    }

    @Override
    public int getWriteOps() {
        return writeOps;
    }

    @Override
    public int getReadOps() {
        return readOps;
    }

    @Override
    public String getWorkerID() {
        return workerID;
    }

    /*
     * @Override public String getRawResults() { return rawData.RawData(); }
     */

}
