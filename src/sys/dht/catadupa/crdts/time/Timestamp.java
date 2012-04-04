package sys.dht.catadupa.crdts.time;

import java.io.Serializable;

/**
 * Common base interface for timestamps.
 * <p>
 */
public interface Timestamp extends Serializable, Comparable<Timestamp> {

	public String siteId();

	public Timestamp clone();

	public int size();

	public void recordIn(CausalityClock<?, ?> clock);

	public boolean includedIn(CausalityClock<?, ?> clock);

}
