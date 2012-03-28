package swift.crdt.interfaces;

public interface TxnLocalCRDT<V extends CRDT<V>> {
    /**
     * Returns the TxnHandle to which the CRDT is currently associated.
     * <p>
     * Returned transaction handler offers snapshot point clock and allows to
     * generate new update operations.
     * 
     * @return
     */
    TxnHandle getTxnHandle();

    /**
     * Returns the plain object corresponding to the CRDT as given in the
     * current state of the txn to which is it associated.
     * 
     * @return
     */
    Object getValue();
}
