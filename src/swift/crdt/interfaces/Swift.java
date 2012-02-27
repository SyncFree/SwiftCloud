package swift.crdt.interfaces;

/**
 * API for the Swift system.
 * 
 * @author annettebieniusa
 */
public interface Swift {
    /**
     * Starts a new transactions.
     * 
     * @param cp
     *            cache policy to be used for the new transaction
     *            TODO(mzawirski): specify how it affects visibility of
     *            concurrently committing transaction?
     * @param readOnly
     *            must be set to true if new transaction is read-only
     * @return TxnHandle for the new transaction
     * 
     */
    TxnHandle beginTxn(CachePolicy cp, boolean readOnly);
}
