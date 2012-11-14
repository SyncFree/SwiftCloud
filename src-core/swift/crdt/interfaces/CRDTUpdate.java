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

import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;

/**
 * Basic interface for representing an update operation on a CRDT
 * 
 * @author nmp, annettebieniusa
 */
public interface CRDTUpdate<V extends CRDT<V>> {

    /**
     * Returns the id associated to the operations.
     */
    TripleTimestamp getTimestamp();

    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);

    // Dirty tricks: sharing and unsharing a timestamp mapping instance to save
    // space and message size... and avoid troubles.
    /**
     * Sets new timestamp mapping for a timestamp
     * 
     * @param mapping
     *            a timestamp mapping reference, compatible with the orginal one
     */
    void setTimestampMapping(TimestampMapping mapping);
}
