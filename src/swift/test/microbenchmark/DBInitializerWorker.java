package swift.test.microbenchmark;

import java.util.Random;

import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.test.microbenchmark.interfaces.MicroBenchmarkWorker;
import swift.test.microbenchmark.interfaces.ResultHandler;
import swift.test.microbenchmark.interfaces.WorkerManager;

public class DBInitializerWorker implements MicroBenchmarkWorker {

    private WorkerManager manager;
    private Swift clientServer;
    private Random random;
    private CRDTIdentifier[] identifiers;

    protected long startTime, endTime;

    public DBInitializerWorker(WorkerManager manager, CRDTIdentifier[] identifiers, Random random, Swift clientServer) {
        this.manager = manager;
        this.identifiers = identifiers;
        this.random = random;
        this.clientServer = clientServer;
    }

    @Override
    public void run() {
        manager.onWorkerStart(this);
        startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < identifiers.length; i++) {
                TxnHandle txh = clientServer.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION,
                        CachePolicy.STRICTLY_MOST_RECENT, false);
                IntegerTxnLocal integer = (IntegerTxnLocal) txh.get(identifiers[i], true, IntegerVersioned.class);
                integer.add(random.nextInt());
                txh.commit();
            }

        } catch (NetworkException e) {
            e.printStackTrace();
        } catch (WrongTypeException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        manager.onWorkerFinish(this);

    }

    @Override
    public ResultHandler getResults() {
        return new DBInitializerResultHandler(this);
    }

    @Override
    public void stop() {

    }

    @Override
    public String getWorkerID() {
        return "INITIALIZER";
    }
}

class DBInitializerResultHandler implements ResultHandler {

    double executionTime;

    public DBInitializerResultHandler(DBInitializerWorker worker) {
        executionTime = (worker.endTime - worker.startTime) / 1000d;
    }

    @Override
    public String toString() {
        String results = "***DB initializer Results***\n";
        results += "Execution Time:\t" + executionTime + "s";
        return results;
    }

}
