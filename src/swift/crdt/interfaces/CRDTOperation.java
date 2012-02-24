package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

/**
 * Basic interface for representing an operation in a CRDT
 * 
 * @author nmp, annettebieniusa
 */
public interface CRDTOperation {

	/**
	 * Get the identifier of CRDT on which operation is to be executed.
	 * 
	 * @return CRDT identifier on which operation is to be executed
	 */
	CRDTIdentifier getTargetUID();

	/**
	 * Returns the timestamp associated to the operations.
	 */
	Timestamp getTimestamp();

	/**
	 * Sets the timestamp for the operation.
	 * 
	 * @param ts
	 */
	void setTimestamp(Timestamp ts);

	/**
	 * Returns the causality clock for the objects on which the operation is to
	 * be executed.
	 * 
	 * @return causality clock for object state on which operation is executed
	 */
	CausalityClock getDependency();
}
