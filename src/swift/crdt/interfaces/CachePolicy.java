package swift.crdt.interfaces;

/**
 * Cache policies used by client for transactional reads.
 * 
 * @author annettebieniusa
 * 
 */
public enum CachePolicy {
    /**
     * Always get the most recent version allowed by {@link IsolationLevel},
     * fail if communication is impossible.
     */
    STRICTLY_MOST_RECENT,
    /**
     * Try to get the most recent version allowed by {@link IsolationLevel}, if
     * impossible fall back to reuse object from the local cache. Fail if cache
     * does not contain compatible version.
     */
    MOST_RECENT,
    /**
     * Reuse object from the local cache whenever possible, yielding possibly
     * stale versions. Minimises connections with server. Fail if cache does not
     * contain compatible version.
     */
    CACHED;
    /*
     * TODO(mzawirski): it seems a common requirement is to have something like
     * CACHED+trigger temporary updates subscription; CACHED mode without
     * updates subscription is a pretty rare case.
     */
}
