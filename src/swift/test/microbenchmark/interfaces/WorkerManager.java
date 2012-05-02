package swift.test.microbenchmark.interfaces;

import swift.test.microbenchmark.RawDataCollector;

public interface WorkerManager {
    
    public void onWorkerStart(MicroBenchmarkWorker worker);
    public void onWorkerFinish(MicroBenchmarkWorker worker);
    public RawDataCollector getNewRawDataCollector(String workerName, int runCOunt);

}
