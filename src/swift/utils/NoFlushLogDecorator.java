package swift.utils;

/**
 * TransactionalLog decorator that discard all flush requests.
 */
public class NoFlushLogDecorator implements TransactionsLog {
    private TransactionsLog implementation;

    public NoFlushLogDecorator(TransactionsLog implementation) {
        this.implementation = implementation;
    }

    @Override
    public void writeEntry(long transactionId, Object entry) {
        implementation.writeEntry(transactionId, entry);
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() {
        implementation.close();
    }
}
