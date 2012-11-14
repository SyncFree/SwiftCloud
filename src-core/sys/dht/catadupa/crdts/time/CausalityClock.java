/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.dht.catadupa.crdts.time;

import java.io.Serializable;
import java.util.Collection;

import swift.exceptions.IncompatibleTypeException;

/**
 * Interface for clocks that allow to trace causality, such as version vector
 * and dotted version vectors
 * 
 * @author nmp
 * @butcheredBy smd
 */
public interface CausalityClock<V extends CausalityClock<V, T>, T extends Timestamp> extends Serializable {

	enum CMP_CLOCK {
		CMP_EQUALS, CMP_DOMINATES, CMP_ISDOMINATED, CMP_CONCURRENT
	};

	/**
	 * Records the next event. <br>
	 * 
	 * @param siteid
	 *            Site identifier.
	 * @return Returns an event clock.
	 * @throws IncompatibleTypeException
	 */
	T recordNext(String siteId);

	/**
	 * Records the next event.
	 * 
	 * @param ec
	 *            Event clock.
	 * @throws IncompatibleTypeException
	 */
	void record(T ec);

	/**
	 * Checks if a given event clock is reflected in this clock
	 * 
	 * @param t
	 *            Event clock.
	 * @return Returns true if the given event clock is included in this
	 *         causality clock.
	 * @throws IncompatibleTypeException
	 */
	boolean includes(T t);

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
	CMP_CLOCK compareTo(V c);

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
	V clone();

	/**
	 * Delivers an iterator of the most recent events for each site.
	 * 
	 * @return Returns the iterator containing the EventClocks for each site.
	 * @throws IncompatibleTypeException
	 */

	public Collection<T> delta(V other);
}
