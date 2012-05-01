package swift.test.microbenchmark;

import java.util.List;
import java.util.Random;

import org.apache.http.ReasonPhraseCatalog;
import org.codehaus.jackson.Versioned;

import swift.application.social.User;
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

    public SwiftExecutorWorker(WorkerManager manager, String workerID, CRDTIdentifier[] identifiers,
            double updateRatio, Random random, Swift clientServer, int maxTxSize) {
        this.manager = manager;
        this.identifiers = identifiers;
        this.updateRatio = updateRatio;
        this.workerID = workerID;
        this.random = random;
        this.clientServer = clientServer;
        this.maxTxSize = maxTxSize;

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
                    TxnHandle txh = clientServer.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION,
                            CachePolicy.STRICTLY_MOST_RECENT, false);
                    int randomIndex = (int) Math.floor(random.nextDouble() * identifiers.length);
                    IntegerTxnLocal integerCRDT = txh.get(identifiers[randomIndex], false, IntegerVersioned.class);
                    if (random.nextDouble() > 0.5) {
                        integerCRDT.add(10);
                    } else {
                        integerCRDT.sub(10);
                    }
                    txh.commit();
                    writeOps++;
                    break;
                }
                case READ_ONLY: {
                    int txSize = (int) Math.ceil(random.nextDouble() * maxTxSize);
                    TxnHandle txh = clientServer.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION,
                            CachePolicy.STRICTLY_MOST_RECENT, true);
                    for (int i = 0; i < txSize; i++) {
                        int randomIndex = (int) Math.floor(Math.random() * identifiers.length);
                        txh.get(identifiers[randomIndex], false, IntegerVersioned.class);
                        readOps++;
                    }
                    txh.commit();
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
        endTime = System.currentTimeMillis();
        manager.onWorkerFinish(this);

    }
    
    @Override
    public ResultHandler getResults() {
        return new OperationExecutorResultHandler(this);
    }

    @Override
    public void stop() {
        stop = true;
        //clientServer.stop(true);
    }

    @Override
    public String getWorkerID() {
        return workerID;
    }
}

class OperationExecutorResultHandler implements ResultHandler {

    double executionTime;
    String workerID;
    int numExecutedTransactions, writeOps, readOps;

    public OperationExecutorResultHandler(SwiftExecutorWorker worker) {
        executionTime = (worker.endTime - worker.startTime) / 1000d;
        workerID = worker.getWorkerID();
        readOps = worker.readOps;
        writeOps = worker.writeOps;
        numExecutedTransactions = (int) worker.numExecutedTransactions;
    }

    @Override
    public String toString() {
        String results = "Worker Results:\n";
        results += "Execution Time:\t" + executionTime + "s" + "\n";
        results += "Executed Transactions:\t" + numExecutedTransactions + " W:\t" + writeOps + "\tR:\t" + readOps
                + "\n";
        results += "Throughput(Tx/min):\t" + numExecutedTransactions / ((executionTime) / 60d) + "\n";
        return results;
    }

}
