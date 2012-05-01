package swift.test.microbenchmark;

import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.http.ReasonPhraseCatalog;
import org.codehaus.jackson.Versioned;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.esotericsoftware.kryo.Kryo;

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

public class RiakExecutorWorker implements MicroBenchmarkWorker {

    private WorkerManager manager;
    private IRiakClient clientServer;
    private Integer[] identifiers;
    private double updateRatio;
    private String workerID;
    private int maxTxSize;
    private Random random;
    private boolean stop;
    private Kryo kryo;

    protected long startTime, endTime;
    protected int numExecutedTransactions, writeOps, readOps;
    private RawDataCollector rawData;

    public RiakExecutorWorker(WorkerManager manager, String workerID, Integer[] identifiers, double updateRatio,
            Random random, IRiakClient clientServer, int maxTxSize) {
        this.manager = manager;
        this.identifiers = identifiers;
        this.updateRatio = updateRatio;
        this.workerID = workerID;
        this.random = random;
        this.clientServer = clientServer;
        this.maxTxSize = maxTxSize;
        kryo = new Kryo();
        rawData = manager.getNewRawDataCollector(workerID);

    }

    @Override
    public void run() {
        manager.onWorkerStart(this);
        startTime = System.currentTimeMillis();

        while (!stop) {
            try {
                OpType operationType = (random.nextDouble() > updateRatio) ? OpType.READ_ONLY : OpType.UPDATE;
                ByteBuffer bb = ByteBuffer.allocate(1024);
                switch (operationType) {

                case UPDATE: {
                    long txStartTime = System.nanoTime();
                    int randomIndex = (int) Math.floor(random.nextDouble() * identifiers.length);
                    IRiakObject riakObj = clientServer.fetchBucket(RiakMicroBenchmark.TABLE_NAME).execute()
                            .fetch("object" + randomIndex).execute();
                    bb = ByteBuffer.wrap(riakObj.getValue());
                    Integer objValue = (Integer) kryo.readObject(bb, Integer.class);

                    if (random.nextDouble() > 0.5) {
                        objValue += 10;
                    } else {
                        objValue -= 10;
                    }
                    bb.clear();
                    kryo.writeObject(bb, objValue);
                    bb.flip();
                    riakObj.setValue(bb.array());
                    clientServer.fetchBucket(RiakMicroBenchmark.TABLE_NAME).execute().store(riakObj);
                    long txEndTime = System.nanoTime();
                    rawData.registerOperation(txEndTime - txStartTime, 1, 1, txStartTime);
                    writeOps++;
                    break;
                }
                case READ_ONLY: {
                    long txStartTime = System.nanoTime();
                    int txSize = (int) Math.ceil(random.nextDouble() * maxTxSize);
                    for (int i = 0; i < txSize; i++) {
                        int randomIndex = (int) Math.floor(Math.random() * identifiers.length);
                        IRiakObject riakObj = clientServer.fetchBucket(RiakMicroBenchmark.TABLE_NAME).execute()
                                .fetch("object" + randomIndex).execute();
                        bb = ByteBuffer.wrap(riakObj.getValue());
                        Integer objValue = (Integer) kryo.readObject(bb, Integer.class);
                        readOps++;
                    }
                    long txEndTime = System.nanoTime();
                    rawData.registerOperation(txEndTime - txStartTime, 0, txSize, txStartTime);
                    break;
                }
                default:
                    break;
                }
                bb.clear();
                numExecutedTransactions++;

            } catch (UnresolvedConflictException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (RiakRetryFailedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ConversionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        endTime = System.currentTimeMillis();
        manager.onWorkerFinish(this);

    }

    @Override
    public ResultHandler getResults() {
        return new RiakOperationExecutorResultHandler(this);
    }

    @Override
    public void stop() {
        stop = true;
        // clientServer.stop(true);
    }

    @Override
    public String getWorkerID() {
        return workerID;
    }

    @Override
    public RawDataCollector getRawData() {
        return rawData;
    }
}

class RiakOperationExecutorResultHandler implements ResultHandler {

    private double executionTime;
    private String workerID;
    private int numExecutedTransactions, writeOps, readOps;
    private RawDataCollector rawData;

    public RiakOperationExecutorResultHandler(RiakExecutorWorker worker) {
        executionTime = (worker.endTime - worker.startTime);
        workerID = worker.getWorkerID();
        readOps = worker.readOps;
        writeOps = worker.writeOps;
        numExecutedTransactions = (int) worker.numExecutedTransactions;
        this.rawData = worker.getRawData();
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
        // TODO Auto-generated method stub
        return workerID;
    }

    @Override
    public String getRawResults() {
        return rawData.RawData();
    }

}
