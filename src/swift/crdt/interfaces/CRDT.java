package swift.crdt.interfaces;

import java.io.Serializable;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

/**
 * Interface for Commutative Replicated Data Types (CRDTs).
 * 
 * @author annettebieniusa
 * 
 * @param <V>
 *            CvRDT type implementing the interface
 */

public interface CRDT<V extends CRDT<V>> extends Serializable {
	/**
	 * Merges the object with other object state. Method is type-invariant such
	 * that only objects of the same type can be merged.
	 * 
	 * @param other
	 *            object state to merge with
	 */
	void merge(V other);

	/**
	 * Invoke an operation on the object.
	 * 
	 * @param op
	 *            operation to be executed
	 */
	void execute(CRDTOperation op);

	/**
	 * Prune the object state to remove meta data from operations dating from
	 * before c.
	 * 
	 * @param c
	 *            time up to which data clean-up is performed
	 */
	void prune(CausalityClock c);

	/**
	 * Remove the effects of the transaction associated to the timestamp.
	 * 
	 * @param ts
	 *            time stamp of transaction that is rolled back.
	 */
	void rollback(Timestamp ts);

	/**
	 * Returns the identifier for the object. Only used by system.
	 */
	CRDTIdentifier getUID();

	/**
	 * Sets the identifier for the object. Only used by system.
	 * 
	 * @param id
	 */
	void setUID(CRDTIdentifier id);

	/**
	 * Returns the causality clock associated to the current object state.
	 * 
	 * @return causality clock associated to object
	 */
	CausalityClock getClock();

	/**
	 * Sets the causality clock that is associated to the current object state.
	 * 
	 * @param c
	 */
	void setClock(CausalityClock c);

	/**
	 * Returns the TxnHandle to which the CRDT is currently associated.
	 * 
	 * @return
	 */
	TxnHandle getTxnHandle();

	/**
	 * Associates the CRDT object to a TxnHandle.
	 * 
	 * @param txn
	 */
	void setTxnHandle(TxnHandle txn);

}
