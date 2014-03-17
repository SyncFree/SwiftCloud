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
package swift.crdt;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * Ordered add-wins set CRDT. WARNING: When adding into a set, make sure that
 * the elements in the set are either immutable or that they are cloned!
 * 
 * @author annettebieniusa ,mzawirski, smduarte
 * 
 * @param <V>
 *            type of elements (must be comparable)
 */
public class AddWinsSortedSetCRDT<V extends Comparable<V>> extends AbstractAddWinsSetCRDT<V, AddWinsSortedSetCRDT<V>> {
    protected SortedMap<V, Set<TripleTimestamp>> elemsInstances;

    // Kryo
    public AddWinsSortedSetCRDT() {
    }

    public AddWinsSortedSetCRDT(CRDTIdentifier id) {
        super(id);
        this.elemsInstances = new TreeMap<V, Set<TripleTimestamp>>();
    }

    private AddWinsSortedSetCRDT(CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock,
            SortedMap<V, Set<TripleTimestamp>> elemsInstances) {
        super(id, txn, clock);
        this.elemsInstances = elemsInstances;
    }

    @Override
    protected Map<V, Set<TripleTimestamp>> getElementsInstances() {
        return elemsInstances;
    }

    @Override
    public AddWinsSortedSetCRDT<V> copy() {
        final SortedMap<V, Set<TripleTimestamp>> newInstances = new TreeMap<V, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elemsInstances, newInstances);
        return new AddWinsSortedSetCRDT<V>(id, txn, clock, newInstances);
    }
}
