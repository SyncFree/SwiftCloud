package swift.test.microbenchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
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
    private String outputDir;
    private int numObjects, cltObjects, maxTxSize, numWorkers, executionTime, runs;
    private Map<String, List<ResultHandler>> results;

    private static final int randomSeed = 1;

    public static final String TABLE_NAME = "BENCHMARK";
    private static final int ESTIMATED_THGPT_MILLIS = 10;
    private static String serverLocation = "127.0.0.1";
    private static int portId = 2001;
    private static Logger logger = Logger.getLogger("swift.benchmark");
    private static CachePolicy cachePolicy;
    private static IsolationLevel isolationLevel;

    public SwiftMicroBenchmark(boolean initialize, int numObjects, int cltObjects, int maxTxSize, int numWorkers,
            double updateRatio, int executionTime, int runs, String outputDir) {
        this.initialize = initialize;
        this.random = new Random(randomSeed);
        this.numObjects = numObjects;
        this.cltObjects = cltObjects;
        this.maxTxSize = maxTxSize;
        this.numWorkers = numWorkers;
        this.updateRatio = updateRatio;
        this.executionTime = executionTime;
        this.runs = runs;
        this.results = new HashMap<String, List<ResultHandler>>();
        this.outputDir = outputDir;
    }

    public static void main(String[] args) {

        int sampleSize, cltSize, maxTxSize, execTime, numRuns, numWorkers;
        String outputDir;
        double updateRatio;
        boolean populate = true;
        if (args.length == 12) {
            if (args[11].equals("-p"))
                populate = true;
        }

        if (args.length < 11 || args.length > 12) {
            System.out
                    .println("[SAMPLE SIZE] [CLT SAMPLE SIZE] [MAX TX SIZE] [NUM WORKERS] [UPDATE RATIO] [EXECUTION TIME SECONDS] [NUM RUNS] [CACHE POLICY] [ISOLATION LEVEL] (SERVER LOCATION, LOCAL) [OUTPUTDIR]");
            return;
        } else {
            sampleSize = Integer.parseInt(args[0]);
            cltSize = Integer.parseInt(args[1]);
            maxTxSize = Integer.parseInt(args[2]);
            numWorkers = Integer.parseInt(args[3]);
            updateRatio = Double.parseDouble(args[4]);
            execTime = Integer.parseInt(args[5]);
            numRuns = Integer.parseInt(args[6]);
            cachePolicy = CachePolicy.valueOf(args[7]);
            isolationLevel = IsolationLevel.valueOf(args[8]);
            if (args[9].equals("LOCAL")) {
                serverLocation = "localhost";
                startSequencer();
                startDCServer();
            }
            else{
                serverLocation = args[9]; 
            }
            outputDir = args[10];
        }
        Sys.init();
        logger.info("SAMPLE SIZE " + sampleSize + " MAX_TX_SIZE " + maxTxSize + " NUM_WORKERS " + numWorkers
                + " UPDATE_RATIO " + updateRatio + " EXECUTION_TIME_SECONDS " + execTime + " NUM_RUNS " + numRuns);
        SwiftMicroBenchmark mb = new SwiftMicroBenchmark(populate, sampleSize, cltSize, maxTxSize, numWorkers,
                updateRatio, 1000 * execTime, numRuns, outputDir);
        try {
            mb.doIt();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void doIt() throws InterruptedException {
        CRDTIdentifier[] identifiers = BenchUtil.generateOpIdentifiers(numObjects);
        SwiftSession client = BenchUtil.getNewSwiftInterface(serverLocation, DCConstants.SURROGATE_PORT);
        if (initialize) {
            stopSemaphore = new Semaphore(-1);
            MicroBenchmarkWorker initializer = new SwiftInitializerWorker(this, identifiers, random, client);
            new Thread(initializer).start();
            try {
                logger.info("START POPULATOR");
                stopSemaphore.acquire();
                client.stopScout(true);
                logger.info("END POPULATOR");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (int r = 0; r < runs; r++) {
            logger.info("WARMING UP FOR " + executionTime / 2 + "ms");
            executeWorkers("WARM_UP", numWorkers, identifiers, cltObjects, executionTime / 2, r, outputDir);
            logger.info("START");
            executeWorkers("SwiftWorker", numWorkers, identifiers, cltObjects, executionTime, r, outputDir);
            logger.info("END");

        }
        printResults();
        System.exit(0);
    }

    private void executeWorkers(String workersName, int numWorkers, CRDTIdentifier[] identifiers, int cltObjects,
            long executionTime, int runCount, String outputDir) throws InterruptedException {
        List<MicroBenchmarkWorker> workers = new ArrayList<MicroBenchmarkWorker>();
        stopSemaphore = new Semaphore(-numWorkers + 1);
        // TODO: Use more then one client?
        // Swift client = BenchUtil.getNewSwiftInterface(serverLocation,
        // DCConstants.SURROGATE_PORT);
        List<CRDTIdentifier> l = new ArrayList<CRDTIdentifier>();
        for (int j = 0; j < identifiers.length; j++)
            l.add(identifiers[j]);
        for (int i = 0; i < numWorkers; i++) {
            CRDTIdentifier[] ids = new CRDTIdentifier[cltObjects];
            Collections.shuffle(l);
            for (int j = 0; j < ids.length; j++)
                ids[j] = l.get(j);
            SwiftSession client = BenchUtil.getNewSwiftInterface(serverLocation, DCConstants.SURROGATE_PORT);
            SwiftExecutorWorker worker = new SwiftExecutorWorker(this, workersName + i, ids, updateRatio, random,
                    client, maxTxSize, cachePolicy, isolationLevel, runCount, outputDir);
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
        if (!workersName.equals("WARM_UP"))
            for (MicroBenchmarkWorker w : workers) {
                w.getRawData().rawDataToFile();
                // System.out.println(w.getRawData().RawData());
            }
    }

    @Override
    public void onWorkerStart(MicroBenchmarkWorker worker) {
        stopSemaphore.release();
        // System.out.println(worker.getWorkerID() + " STARTED");
    }

    @Override
    public void onWorkerFinish(MicroBenchmarkWorker worker) {
        if (worker.getWorkerID().contains("WARM_UP") || worker.getWorkerID().equals("INITIALIZER")) {
            stopSemaphore.release();
            return;
        }

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
            double numExecutedTransactions = 0;
            double writeOps = 0;
            double readOps = 0;
            int runCounter = 1;
            for (ResultHandler run : workerResults.getValue()) {
                if (run instanceof SwiftOperationExecutorResultHandler) {
                    System.out.println("RUN " + runCounter++);
                    SwiftOperationExecutorResultHandler wr = (SwiftOperationExecutorResultHandler) run;
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
        results += "Throughput(Tx/s):\t" + totalExecutedTransactions / ((executionTime / 1000)) + "\n";
        System.out.println(results);
    }

    @Override
    public RawDataCollector getNewRawDataCollector(String workerName, int runCount, String outputDir) {
        int initialSize = (int) (maxTxSize * (1 - updateRatio) + 1) * executionTime * ESTIMATED_THGPT_MILLIS;
        return new RawDataCollector(initialSize, workerName, runCount, outputDir);
    }

    private static void startDCServer() {
        DCServer.main(new String[] { serverLocation });
    }

    private static void startSequencer() {
        DCSequencerServer.main(new String[] { "-name", serverLocation });
    }
}
