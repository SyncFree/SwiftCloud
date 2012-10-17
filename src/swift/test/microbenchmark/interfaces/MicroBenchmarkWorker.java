package swift.test.microbenchmark.interfaces;

import java.util.List;

import swift.crdt.interfaces.SwiftSession;
import swift.test.microbenchmark.BenchOperation;
import swift.test.microbenchmark.RawDataCollector;

public interface MicroBenchmarkWorker extends Runnable {

    void stop();

    ResultHandler getResults();
    
    RawDataCollector getRawData();

    String getWorkerID();

}
