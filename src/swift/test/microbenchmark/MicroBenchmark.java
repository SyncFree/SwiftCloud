package swift.test.microbenchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Semaphore;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.test.microbenchmark.interfaces.MicroBenchmarkWorker;
import swift.test.microbenchmark.interfaces.ResultHandler;
import swift.test.microbenchmark.interfaces.WorkerManager;
import sys.Sys;

public class MicroBenchmark implements WorkerManager {

    private boolean initialize;
    private Semaphore stopSemaphore;
    private Random random;
    private double updateRatio;
    private int numObjects, maxTxSize, numWorkers, executionTime, runs;
    private Map<String, List<ResultHandler>> results;

    private static final int /* valueLength = 20, valueLengthDeviation = 0 , */randomSeed = 1;

    public static final String TABLE_NAME = "BENCHMARK";
    private static String serverLocation = "localhost";
    private static int portId = 2001;

    public MicroBenchmark(boolean initialize, int numObjects, int maxTxSize, int numWorkers, double updateRatio,
            int executionTime, int runs) {
        this.initialize = initialize;
        this.random = new Random(randomSeed);
        this.numObjects = numObjects;
        this.maxTxSize = maxTxSize;
        this.numWorkers = numWorkers;
        this.updateRatio = updateRatio;
        this.executionTime = executionTime;
        this.runs = runs;
        this.results = new HashMap<String, List<ResultHandler>>();
    }

    public static void main(String[] args) {

        int sampleSize, maxTxSize, execTime, numRuns, numWorkers;
        double updateRatio;
        boolean populate = false;
        if (args.length == 7) {
            if (args[6].equals("-p"))
                populate = true;
        }

        if (args.length < 6 || args.length > 7) {
            System.out
                    .println("[SAMPLE SIZE] [MAX TX SIZE] [NUM WORKERS] [UPDATE RATIO] [EXECUTION TIME SECONDS] [NUM RUNS]");
            return;
        } else {
            sampleSize = Integer.parseInt(args[0]);
            maxTxSize = Integer.parseInt(args[1]);
            numWorkers = Integer.parseInt(args[2]);
            updateRatio = Double.parseDouble(args[3]);
            execTime = Integer.parseInt(args[4]);
            numRuns = Integer.parseInt(args[5]);
        }
        Sys.init();
        MicroBenchmark mb = new MicroBenchmark(populate, sampleSize, maxTxSize, numWorkers, updateRatio,
                1000 * execTime, numRuns);
        try {
            mb.doIt();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void doIt() throws InterruptedException {
        CRDTIdentifier[] identifiers = BenchUtil.generateOpIdentifiers(numObjects);

        if (initialize) {
            stopSemaphore = new Semaphore(-1);
            Swift client = BenchUtil.getNewSwiftInterface(serverLocation, portId);
            MicroBenchmarkWorker initializer = new DBInitializerWorker(this, identifiers, random, client);
            new Thread(initializer).start();
            try {
                stopSemaphore.acquire();
                // FIXME: Blocks here
                // System.out.println("STOP CLIENT");
                // client.stop(false);
                // System.out.println("CLIENT STOPPED");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (int r = 0; r < runs; r++) {
            List<MicroBenchmarkWorker> workers = new ArrayList<MicroBenchmarkWorker>();
            stopSemaphore = new Semaphore(-numWorkers + 1);
            for (int i = 0; i < numWorkers; i++) {
                Swift client = BenchUtil.getNewSwiftInterface(serverLocation, portId);
                OperationExecutorWorker worker = new OperationExecutorWorker(this, "worker" + i, identifiers,
                        updateRatio, random, client, maxTxSize);
                new Thread(worker).start();
                workers.add(worker);

            }
            stopSemaphore.acquire();
            Thread.sleep(executionTime);
            stopSemaphore = new Semaphore(-numWorkers + 1);
            for (MicroBenchmarkWorker w : workers) {
                w.stop();
            }
            stopSemaphore.acquire();
        }
        printResults();

    }

    @Override
    public void onWorkerStart(MicroBenchmarkWorker worker) {
        stopSemaphore.release();
        // System.out.println(worker.getWorkerID() + " STARTED");
    }

    @Override
    public void onWorkerFinish(MicroBenchmarkWorker worker) {
        // System.out.println(worker.getResults().toString());
        List<ResultHandler> workerRuns = results.get(worker.getWorkerID());
        if (workerRuns == null) {
            workerRuns = new ArrayList<ResultHandler>();
            results.put(worker.getWorkerID(), workerRuns);
        }
        workerRuns.add(worker.getResults());
        // System.out.println(worker.getWorkerID() + " STOPPED");
        stopSemaphore.release();

    }

    // TODO: Need refactoring to become generic
    private void printResults() {

        double totalExecutedTransactions = 0;
        double totalWriteOps = 0;
        double totalReadOps = 0;

        for (Entry<String, List<ResultHandler>> workerResults : results.entrySet()) {
            String worker = workerResults.getKey();
            if (worker.equals("INITIALIZER"))
                continue;

            String results = worker + " Results:\n";
            double numExecutedTransactions = 0;
            double writeOps = 0;
            double readOps = 0;
            for (ResultHandler run : workerResults.getValue()) {
                if (run instanceof OperationExecutorResultHandler) {
                    OperationExecutorResultHandler wr = (OperationExecutorResultHandler) run;
                    numExecutedTransactions += wr.numExecutedTransactions / workerResults.getValue().size();
                    writeOps += wr.writeOps / workerResults.getValue().size();
                    readOps += wr.readOps / workerResults.getValue().size();
                }
            }

            results += "Executed Transactions:\t" + numExecutedTransactions + " W:\t" + writeOps + "\tR:\t" + readOps
                    + "\n";
            results += "Throughput(Tx/min):\t" + numExecutedTransactions / ((executionTime / 1000) / 60d) + "\n";
            System.out.println(results);

            totalExecutedTransactions += numExecutedTransactions;
            totalWriteOps += writeOps;
            totalReadOps += readOps;
        }
        String results = "Total Results:\n";
        results += "Executed Transactions:\t" + totalExecutedTransactions + " W:\t" + totalWriteOps + "\tR:\t"
                + totalReadOps + "\n";
        results += "Throughput(Tx/min):\t" + totalExecutedTransactions / ((executionTime / 1000) / 60d) + "\n";
        System.out.println(results);
    }

}
