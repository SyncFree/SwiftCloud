package swift.crdt.interfaces;

/**
 * Cache policies used by client for a transaction handle.
 * 
 * @author annettebieniusa
 * 
 */
// TODO(mzawirski): update descriptions, consider combinations with
// IsolationLevel (the semantics should be clear).
public enum CachePolicy {
    /**
     * CACHED: (Re-)Use the objects currently stored in cache.
     * 
     * MOST_RECENT: Tries to update the cache for the next transaction.
     * TransactionHandler uses currently cached objects if update fails.
     * 
     * STRICTLY_MOST_RECENT: Tries to update the cache for the next transaction.
     * Strict mode will fail if server cannot be contacted.
     */

    STRICTLY_MOST_RECENT, MOST_RECENT, CACHED;
}
