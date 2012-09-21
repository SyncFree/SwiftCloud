package swift.crdt.interfaces;

import swift.clocks.CausalityClock;

// WISHME: separate client and system interface (needed for mocks)
// TODO: Extend interface to offer optional buffering of transaction's updates (e.g. to aggregate increments, assignments etc.)
// with explicit transaction close.
public interface TxnLocalCRDT<V extends CRDT<V>> {
    /**
     * Returns the plain object corresponding to the CRDT as given in the
     * current state of the txn to which is it associated.
     * 
     * @return
     */
    Object getValue();

    /**
     * Returns the TxnHandle to which the CRDT is currently associated.
     * <p>
     * <b>INTERNAL, SYSTEM USE:</b> returned transaction allows to generate and
     * register new update operations.
     * 
     * @return
     */
    TxnHandle getTxnHandle();

    /**
     * <b>INTERNAL, SYSTEM USE:</b>
     * 
     * @return constant snapshot clock of this local object representation
     */
    CausalityClock getClock();

    Object executeQuery(CRDTQuery<V> query);
}
