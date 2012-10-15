package swift.utils;

/**
 * Durable sequential log. Implementations must be thread-safe.
 * 
 * @author mzawirski
 */
public interface TransactionsLog {
    /**
     * Atomically stores provided entries.
     * 
     * @param transactionId
     *            id of the transactions writing
     * @param entry
     *            object to write in the log, possibly asynchronously
     */
    void writeEntry(final long transactionId, Object entry);

    /**
     * Flushes the log, so all {@link #writeEntry(Object)} are guaranteed to be
     * durable at this time.
     */
    void flush();

    /**
     * Closes the log.
     */
    void close();
}