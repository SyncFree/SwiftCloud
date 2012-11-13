package sys.dht.catadupa.crdts;

import java.io.Serializable;

/**
 * Interface for Convergent Replicated Data Types (CvRDTs) supporting
 * (type-invariant) merge function.
 * 
 * @author annettebieniusa butcheredBy smd
 * 
 * @param <V>
 *            CvRDT type implementing the interface
 * 
 */
public interface CvRDT<V> extends Serializable {

	void merge(V other);

	// <C extends CausalityClock<C>> void merge(V that,
	// CausalityClock<C> thisClock, CausalityClock<C> thatClock)
	// throws IncompatibleTypeException;

}
