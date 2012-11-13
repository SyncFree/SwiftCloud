package swift.test.microbenchmark.interfaces;

import swift.test.microbenchmark.RawDataCollector;

public interface ResultHandler {
       
    public double getExecutionTime();
    public int getNumExecutedTransactions();
    public int getWriteOps();
    public int getReadOps();
    public String getWorkerID();
//    public String getRawResults();
   
}
