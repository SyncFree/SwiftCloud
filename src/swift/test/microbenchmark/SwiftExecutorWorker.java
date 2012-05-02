package swift.test.microbenchmark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.http.ReasonPhraseCatalog;
import org.codehaus.jackson.Versioned;

import swift.application.social.User;
import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetTxnLocalString;
import swift.crdt.SetVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.test.microbenchmark.interfaces.MicroBenchmarkWorker;
import swift.test.microbenchmark.interfaces.ResultHandler;
import swift.test.microbenchmark.interfaces.WorkerManager;
import swift.test.microbenchmark.objects.StringCopyable;
import swift.utils.NanoTimeCollector;
import sys.Sys;
import sys.dht.catadupa.crdts.ORSet;

public class SwiftExecutorWorker implements MicroBenchmarkWorker {

    private WorkerManager manager;
    private Swift clientServer;
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

    public SwiftExecutorWorker(WorkerManager manager, String workerID, CRDTIdentifier[] identifiers,
            double updateRatio, Random random, Swift clientServer, int maxTxSize, CachePolicy cachePolicy,
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
                    int randomIndex = (int) Math.floor(random.nextDouble() * identifiers.length);
                    IntegerTxnLocal integerCRDT = txh.get(identifiers[randomIndex], false, IntegerVersioned.class,
                            new MicroBenchmarkUpdateListener());
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
                        txh.get(identifiers[randomIndex], false, IntegerVersioned.class,
                                new MicroBenchmarkUpdateListener());
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
        //System.out.println("STOP CLIENT");
        clientServer.stop(true);
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
        public void onObjectUpdate(TxnHandle txn_old, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
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

    @Override
    public String getRawResults() {
        return rawData.RawData();
    }


}
