package swift.crdt.interfaces;

import swift.client.SwiftImpl;
import swift.exceptions.NetworkException;

/**
 * API for the Swift system, a client session that can issue transactions. A
 * session is normally attached to a scout ({@link SwiftScout}). See
 * {@link SwiftImpl} to learn how to start a scout and session. Note that all
 * "session guarantees" apply to this unit of session.
 * 
 * @author annettebieniusa, mzawirski
 * @see SwiftImpl
 */
public interface SwiftSession {
    /**
     * Starts a new transaction, observing the results of locally committed
     * transactions in this session, and some external transactions committed to
     * the store, depending on cache and isolation options.
     * 
     * @param isolationLevel
     *            isolation level defining consistency guarantees for
     *            transaction reads
     * @param cachePolicy
     *            cache policy for the new transaction
     * @param readOnly
     *            when true, the transaction cannot generate any updates or
     *            create objects; recommended for read-only transactions for
     *            better performance
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
     * Stops the underlying scout, which renders it unusable after this call
     * returns. Careful, it may also affect other sessions of this scout.
     * 
     * @param waitForCommit
     *            when true, this call blocks until all locally committed
     *            transactions commit globally in the store
     */
    void stopScout(boolean waitForCommit);

    // TODO: change to stopSession() and count opened sessions?

    /**
     * @return session identifier
     */
    String getSessionId();

    /**
     * @return scout associated with this session
     */
    SwiftScout getScout();
}
