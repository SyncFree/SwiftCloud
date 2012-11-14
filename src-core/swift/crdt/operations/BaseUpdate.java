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
package swift.crdt.operations;

import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;

public abstract class BaseUpdate<V extends CRDT<V>> implements CRDTUpdate<V> {
    private TripleTimestamp ts;

    // required by kryo
    protected BaseUpdate() {
    }

    protected BaseUpdate(TripleTimestamp ts) {
        this.ts = ts;
    }

    @Override
    public TripleTimestamp getTimestamp() {
        return this.ts;
    }

    @Override
    public void setTimestampMapping(final TimestampMapping mapping) {
        ts = ts.copyWithMappings(mapping);
    }

    /**
     * Applies operation to the given object instance. Importantly, any newly
     * used timestamp mapping should be registered through in
     * {@link BaseCRDT#registerTimestampUsage(TripleTimestamp)}.
     * 
     * @param crdt
     *            object where operation is applied
     */
    public abstract void applyTo(V crdt);
}
