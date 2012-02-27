package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.exceptions.InvalidParameterException;

/**
 * Source for generating new timestamps.
 * 
 * @author nmp
 */
public interface TimestampSource<T> {
	/**
	 * Generates a new timestamp
	 * 
	 * @return
	 * @throws InvalidParameterException 
	 */
	T generateNew() throws InvalidParameterException;

	/**
	 * Generates a new timestamp with respect to the causality clock given as
	 * parameter.
	 * 
	 * @return
	 */
	<V extends CausalityClock<V>> T generateNew(CausalityClock<V> c);
}
