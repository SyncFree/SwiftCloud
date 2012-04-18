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
     * the transaction.
     */
    SNAPSHOT_ISOLATION,
    /**
     * Transaction reads from a snapshot which may be inconsistent. Reading the
     * same object twice (through
     * {@link TxnHandle#get(swift.crdt.CRDTIdentifier, boolean, Class)}) yields
     * the same result.
     */
    REPEATABLE_READS,
    /**
     * Transaction reads from committed transactions. Reading the same object
     * twice may yield different results.
     */
    READ_COMMITTED
}
