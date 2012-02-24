package swift.crdt.interfaces;

import swift.clocks.CausalityClock;

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
	 */
	T generateNew();

	/**
	 * Generates a new timestamp with respect to the causality clock given as
	 * parameter.
	 * 
	 * @return
	 */
	<V extends CausalityClock<V>> T generateNew(CausalityClock<V> c);
}
