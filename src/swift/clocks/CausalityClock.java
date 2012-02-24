package swift.clocks;

import java.io.Serializable;

import swift.exceptions.IncompatibleTypeException;
import swift.exceptions.InvalidParameterException;

/**
 * Interface for clocks that allow to trace causality, such as version vector
 * and dotted version vectors
 * 
 * @author nmp
 */
public interface CausalityClock<V extends CausalityClock<V>> extends
		Serializable {
	enum CMP_CLOCK {
		CMP_EQUALS, CMP_DOMINATES, CMP_ISDOMINATED, CMP_CONCURRENT
	};

	/**
	 * Records the next event.
	 * 
	 * @param ec
	 *            Event clock.
	 * @throws IncompatibleTypeException
	 */
	void record(Timestamp ec) throws InvalidParameterException;

	/**
	 * Checks if a given event clock is reflected in this clock
	 * 
	 * @param t
	 *            Event clock.
	 * @return Returns true if the given event clock is included in this
	 *         causality clock.
	 * @throws IncompatibleTypeException
	 */
	boolean includes(Timestamp t) throws IncompatibleTypeException;

	/**
	 * Returns the most recent event for a given site. <br>
	 * 
	 * @param siteid
	 *            Site identifier.
	 * @return Returns an event clock.
	 */
	Timestamp getLatest(String siteid);

	/**
	 * Returns the most recent event for a given site. <br>
	 * 
	 * @param siteid
	 *            Site identifier.
	 * @return Returns an event clock.
	 */
	long getLatestCounter(String siteid);

	/**
	 * Compares two causality clock.
	 * 
	 * @param c
	 *            Clock to compare to
	 * @return Returns one of the following:<br>
	 *         CMP_EQUALS : if clocks are equal; <br>
	 *         CMP_DOMINATES : if this clock dominates the given c clock; <br>
	 *         CMP_ISDOMINATED : if this clock is dominated by the given c
	 *         clock; <br>
	 *         CMP_CONCUREENT : if this clock and the given c clock are
	 *         concurrent; <br>
	 * @throws IncompatibleTypeException
	 */
	CMP_CLOCK compareTo(V c) throws IncompatibleTypeException;

	/**
	 * Merge this clock with the given c clock.
	 * 
	 * @param c
	 *            Clock to merge to
	 * @return Returns one of the following, based on the initial value of
	 *         clocks:<br>
	 *         CMP_EQUALS : if clocks were equal; <br>
	 *         CMP_DOMINATES : if this clock dominated the given c clock; <br>
	 *         CMP_ISDOMINATED : if this clock was dominated by the given c
	 *         clock; <br>
	 *         CMP_CONCUREENT : if this clock and the given c clock were
	 *         concurrent; <br>
	 */
	CMP_CLOCK merge(V c);

	/**
	 * Create a copy of this causality clock.
	 */
	CausalityClock<V> clone();

	/**
	 * Delivers an iterator of the most recent events for each site.
	 * 
	 * @return Returns the iterator containing the EventClocks for each site.
	 * @throws IncompatibleTypeException
	 */

}
