/*****************************************************************************
 * Copyright 2011-2014 INRIA
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
package swift.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * Add-wins set CRDT. WARNING: When adding into a set, make sure that the
 * elements in the set are either immutable or that they are cloned!
 * 
 * @author annettebieniusa ,mzawirski, smduarte
 * 
 * @param <V>
 *            type of elements, immutable during the set lifetime
 */
public class AddWinsSetCRDT<V> extends AbstractAddWinsSetCRDT<V, AddWinsSetCRDT<V>> {
    protected Map<V, Set<TripleTimestamp>> elemsInstances;

    // Kryo
    public AddWinsSetCRDT() {
    }

    public AddWinsSetCRDT(CRDTIdentifier id) {
        super(id);
        this.elemsInstances = new HashMap<V, Set<TripleTimestamp>>();
    }

    private AddWinsSetCRDT(CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock,
            Map<V, Set<TripleTimestamp>> elemsInstances) {
        super(id, txn, clock);
        this.elemsInstances = elemsInstances;
    }

    @Override
    protected Map<V, Set<TripleTimestamp>> getElementsInstances() {
        return elemsInstances;
    }

    @Override
    public AddWinsSetCRDT<V> copy() {
        final HashMap<V, Set<TripleTimestamp>> newInstances = new HashMap<V, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elemsInstances, newInstances);
        return new AddWinsSetCRDT<V>(id, txn, clock, newInstances);
    }
}
