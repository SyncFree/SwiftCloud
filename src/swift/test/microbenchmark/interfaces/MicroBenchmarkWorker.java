package swift.test.microbenchmark.interfaces;

import java.util.List;

import swift.crdt.interfaces.Swift;
import swift.test.microbenchmark.BenchOperation;

public interface MicroBenchmarkWorker extends Runnable {

    void stop();

    ResultHandler getResults();

    String getWorkerID();

}
