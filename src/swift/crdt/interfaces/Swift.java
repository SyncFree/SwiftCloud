package swift.crdt.interfaces;

import swift.exceptions.NetworkException;

/**
 * API for the Swift system.
 * 
 * @author annettebieniusa
 */
public interface Swift {
    /**
     * Starts a new transaction, observing the results of previously locally
     * committed transactions (read-your-writes) and all their dependencies
     * (monotonic-reads), and some external transactions committed to the store,
     * depending on cache and isolation options.
     * 
     * @param isolationLevel
     *            isolation level defining consistency guarantees for
     *            transaction reads
     * @param cachePolicy
     *            cache policy for the new transaction
     * @param readOnly
     *            when true, perform updates on objects from the transaction
     *            will render an error
     * @return TxnHandle for the new transaction
     * @throws IllegalStateException
     *             when another transaction is pending in the system, or the
     *             client is already stopped
     * @throws NetworkException
     *             when strict cachePolicy is selected and the store does not
     *             reply
     */
    TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy, boolean readOnly)
            throws NetworkException;

    // WISHME: in order to support disconnected operations w/client partial
    // replication, extend API to start transaction and prefetch/update some
    // objects and to switch IsolationLevel/CachePolicy after stating the
    // transaction.

    /**
     * Stops the client, which renders it unusable after this call returns.
     * 
     * @param waitForCommit
     *            when true, this call blocks until all locally committed
     *            transactions commit globally in the store
     */
    void stop(boolean waitForCommit);
}
