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
     * Generates a new timestamp.
     * 
     * @return
     */
    <V extends CausalityClock<V>> T generateNew();

}
