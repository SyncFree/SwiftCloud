package swift.test.microbenchmark.interfaces;

public interface ResultHandler {
       
    public double getExecutionTime();
    public int getNumExecutedTransactions();
    public int getWriteOps();
    public int getReadOps();
    public String getWorkerID();
}
