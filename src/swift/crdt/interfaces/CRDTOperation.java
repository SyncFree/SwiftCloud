package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

/**
 * Basic interface for representing an operation in a CRDT
 * 
 * @author nmp
 */
public interface CRDTOperation {

	CRDTIdentifier getTargetUID();

	/**
	 * 
	 */
	Timestamp getTimestamp();

	void setTimestamp(Timestamp ts);

	/**
	 * 
	 * @return previous state
	 */
	CausalityClock getDependency();
}
