package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.utils.Pair;

/**
 * Basic interface for representing an operation in a CRDT
 * 
 * @author nmp
 */
public interface CRDTOperation {

	Pair<String, String> getTargetUID();

	/**
	 * 
	 */
	Timestamp getTimestamp();

	/**
	 * 
	 * @return previous state
	 */
	CausalityClock getDependency();
}
