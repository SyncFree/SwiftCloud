package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;

/**
 * Interface for transaction handles.
 * 
 * @author annettebieniusa
 * 
 */
public interface TxnHandle<T extends Timestamp> {
	<V extends CRDT<V, I, T>, I> CRDT<V, I, T> get(CRDTIdentifier id,
			boolean create, Class<T> classOfT);

	/**
	 * Commits the transaction.
	 */
	void commit();

	/**
	 * Reverts the updates which were executed under this transaction.
	 */
	void rollback();

	/**
	 * Generates timestamps for operations. Only called by system.
	 * 
	 * @return next timestamp
	 */
	T nextTimestamp();

	/**
	 * Returns the causality clock associated to this transaction handle. Only
	 * called by system.
	 * 
	 * @return
	 */
	CausalityClock getClock();

	/**
	 * Register a new CRDT operation with the transaction. Only called by
	 * system.
	 * 
	 * @param op
	 */
	void registerOperation(CRDTOperation<?, T> op);

}
