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

public class SwiftMicroBenchmark implements WorkerManager {

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

    public SwiftMicroBenchmark(boolean initialize, int numObjects, int maxTxSize, int numWorkers, double updateRatio,
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
        System.out.println("SAMPLE SIZE " + sampleSize + " MAX_TX_SIZE " + maxTxSize + " NUM_WORKERS " + numWorkers
                + " UPDATE_RATIO " + updateRatio + " EXECUTION_TIME_SECONDS " + execTime + " NUM_RUNS " + numRuns);
        SwiftMicroBenchmark mb = new SwiftMicroBenchmark(populate, sampleSize, maxTxSize, numWorkers, updateRatio,
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
            Swift client = BenchUtil.getNewSwiftInterface(serverLocation, DCConstants.SURROGATE_PORT);
            MicroBenchmarkWorker initializer = new SwiftInitializerWorker(this, identifiers, random, client);
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
                Swift client = BenchUtil.getNewSwiftInterface(serverLocation, DCConstants.SURROGATE_PORT);
                SwiftExecutorWorker worker = new SwiftExecutorWorker(this, "worker" + i, identifiers, updateRatio,
                        random, client, maxTxSize);
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

            double numExecutedTransactions = 0;
            double writeOps = 0;
            double readOps = 0;
            int runCounter = 1;
            for (ResultHandler run : workerResults.getValue()) {
                if (run instanceof RiakOperationExecutorResultHandler) {
                    System.out.println("RUN " + runCounter++);
                    RiakOperationExecutorResultHandler wr = (RiakOperationExecutorResultHandler) run;
                    System.out.println(run.toString());
                    numExecutedTransactions += wr.getNumExecutedTransactions() / workerResults.getValue().size();
                    writeOps += wr.getWriteOps() / workerResults.getValue().size();
                    readOps += wr.getReadOps() / workerResults.getValue().size();
                }
            }

            totalExecutedTransactions += numExecutedTransactions;
            totalWriteOps += writeOps;
            totalReadOps += readOps;
        }
        String results = "Mean Total Results:\n";
        results += "Executed Transactions:\t" + totalExecutedTransactions + " W:\t" + totalWriteOps + "\tR:\t"
                + totalReadOps + "\n";
        results += "Throughput(Tx/min):\t" + totalExecutedTransactions / ((executionTime / 1000) / 60d) + "\n";
        System.out.println(results);
    }

}
