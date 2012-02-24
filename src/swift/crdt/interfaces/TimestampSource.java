package swift.crdt.interfaces;

import swift.clocks.CausalityClock;

/**
 * Source for generating new timestamps
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
	 * Generates a new timestamp given that current clock is c
	 * 
	 * @return
	 */
	<V extends CausalityClock<V>> T generateNew(CausalityClock<V> c);
}
