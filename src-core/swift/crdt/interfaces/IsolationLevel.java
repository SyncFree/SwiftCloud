package swift.crdt.interfaces;

/**
 * Definition of isolation levels offered to a transaction, i.e. guarantees for
 * reads made by a transaction.
 * 
 * @author mzawirski
 */
public enum IsolationLevel {
    /**
     * Transaction reads from a consistent snapshot defined at the beginning of
     * the transaction. Transaction observes results of all previously locally
     * committed transactions (read-your-writes) and all previous reads
     * (monotonic-reads)
     * <p>
     * Note that snapshots of different transactions are only partially ordered,
     * not totally ordered. I.e., formally this is Non-Mononotonic Snapshot
     * Isolation.
     */
    SNAPSHOT_ISOLATION,
    /**
     * Transaction reads from a snapshot which may be inconsistent. Reading the
     * same object twice (through
     * {@link TxnHandle#get(swift.crdt.CRDTIdentifier, boolean, Class)}) yields
     * the same result.
     * <p>
     * The system makes best effort to make sure that the transaction observe as
     * much of previously locally committed transactions (read-your-writes) and
     * all previous reads (monotonic-reads) as possible, preferably all.
     */
    // Why only best-effort? Because of potential cache entry eviction and
    // concurrent read request by application.
    REPEATABLE_READS,
    /**
     * Transaction reads from committed transactions. Same as
     * {@link #REPEATABLE_READS}, but reading the same object twice may yield
     * different results.
     */
    // TODO: implement
    READ_COMMITTED,
    /**
     * Transaction reads may observe only partial results of some transaction.
     */
    // TODO: implement
    READ_UNCOMMITTED;
}
