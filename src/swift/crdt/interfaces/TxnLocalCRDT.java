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
    public TxnHandle getTxnHandle();

}
