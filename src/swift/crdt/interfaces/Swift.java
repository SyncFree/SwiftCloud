package swift.crdt.interfaces;

/**
 * API for the Swift system.
 * 
 * @author annettebieniusa
 */
public interface Swift {
    /**
     * Starts a new transactions, observing the results of previously locally
     * committed transactions and external transactions committed to the store.
     * 
     * @param isolationLevel
     *            isolation level defining guarantees for transaction reads
     * @param cachePolicy
     *            cache policy to be used for the new transaction
     * @param readOnly
     *            when true, transaction is read-only
     * @return TxnHandle for the new transaction
     * @throws IllegalStateException
     *             when another transaction is pending in the system, or the
     *             client is stopped
     */
    TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy, boolean readOnly);

    // TODO: in order to support disconnected operations w/client partial
    // replication, extend API to start transaction and prefetch/update some
    // objects.

    /**
     * Stops the client, which renders it unusable after this call returns.
     * 
     * @param waitForCommit
     *            when true, this call blocks until all locally committed
     *            transactions commit globally in the store
     */
    void stop(boolean waitForCommit);
}
