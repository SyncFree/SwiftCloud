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
package swift.crdt.interfaces;

import swift.clocks.CausalityClock;

// WISHME: separate client and system interface (needed for mocks)
// TODO: Extend interface to offer optional buffering of transaction's updates (e.g. to aggregate increments, assignments etc.)
// with explicit transaction close.
/**
 * A modifiable view of an CRDT object version. Updates applied on this view are
 * handed off to the {@link TxnHandle}.
 * 
 * @author mzawirski
 * @param <V>
 *            the type of object that this view is offered for
 */
public interface TxnLocalCRDT<V extends CRDT<V>> {
    /**
     * Returns the plain object corresponding to the CRDT as given in the
     * current state of the txn to which is it associated.
     * 
     * @return
     */
    Object getValue();

    /**
     * Returns the TxnHandle to which the CRDT is currently associated.
     * <p>
     * <b>INTERNAL, SYSTEM USE:</b> returned transaction allows to generate and
     * register new update operations.
     * 
     * @return
     */
    TxnHandle getTxnHandle();

    /**
     * <b>INTERNAL, SYSTEM USE:</b>
     * 
     * @return constant snapshot clock of this local object representation
     */
    CausalityClock getClock();

    /**
     * Executes a query on this version view.
     * 
     * @param query
     *            a query to execute
     * @return query result
     */
    Object executeQuery(CRDTQuery<V> query);
}
