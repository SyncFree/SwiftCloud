package swift.test.microbenchmark.interfaces;

public interface WorkerManager {
    
    public void onWorkerStart(MicroBenchmarkWorker worker);
    public void onWorkerFinish(MicroBenchmarkWorker worker);

}
