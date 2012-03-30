package swift.crdt.interfaces;

/**
 * Status of a SwiftCloud transaction.
 * 
 * @author mzawirski
 */
// TODO: define legal transitions
public enum TxnStatus {
    /**
     * Open and accepts operations.
     */
    PENDING(true, false, false, false),
    /**
     * Cancelled. Does not accept new operations, previously performed
     * operations are lost.
     */
    CANCELLED(false, true, false, false),
    /**
     * Committed locally. Operations are visible to new local transactions, but
     * possibly not propagated to the store.
     */
    COMMITTED_LOCAL(false, true, true, false),
    /**
     * Committed to the store. Operations are (or will be soon) be visible to
     * transactions started at other clients.
     */
    COMMITTED_GLOBAL(false, true, true, true);

    private final boolean acceptingOps;
    private final boolean terminated;
    private final boolean outcomeLocallyVisible;
    private final boolean outcomePubliclyVisible;

    private TxnStatus(final boolean acceptingOps, final boolean terminated, final boolean outcomeLocallyVisible,
            final boolean outcomePubliclyVisible) {
        this.acceptingOps = acceptingOps;
        this.terminated = terminated;
        this.outcomeLocallyVisible = outcomeLocallyVisible;
        this.outcomePubliclyVisible = outcomePubliclyVisible;
    }

    /**
     * @return true if transaction accepts new operations
     */
    public boolean isAcceptingOperations() {
        return acceptingOps;
    }

    /**
     * @return true if transaction has terminated
     */
    public boolean isTerminated() {
        return terminated;
    }

    /**
     * @return true if outcome of a transaction is locally visible
     */
    public boolean isOutcomeLocallyVisible() {
        return outcomeLocallyVisible;
    }

    /**
     * @return true if outcome of a transaction is publicly visible (soft
     *         guarantee)
     */
    public boolean isOutcomePubliclyVisible() {
        return outcomePubliclyVisible;
    }
}
