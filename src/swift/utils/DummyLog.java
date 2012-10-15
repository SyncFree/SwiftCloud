package swift.utils;

/**
 * No-op DurableLog implementation.
 * 
 * @author mzawirski
 */
public class DummyLog implements TransactionsLog {
    @Override
    public void writeEntry(final long transactionId, final Object entry) {
        // no-op
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
