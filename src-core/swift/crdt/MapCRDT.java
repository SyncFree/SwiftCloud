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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * CRDT map. WARNING: When constructing a map, make sure that the elements in
 * the map are either immutable or that they are cloned!
 * 
 * @author vb, annettebieniusa, mzawirsk, smduarte
 * 
 * @param <V>
 */
public class MapCRDT<K, V> extends BaseCRDT<MapCRDT<K, V>> {

    private static final long serialVersionUID = 1L;
    protected Map<K, Map<V, Set<TripleTimestamp>>> keysToElementsInstances;

    // Kryo
    public MapCRDT() {
    }

    public MapCRDT(CRDTIdentifier id) {
        super(id);
        keysToElementsInstances = new HashMap<K, Map<V, Set<TripleTimestamp>>>();
    }

    private MapCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            Map<K, Map<V, Set<TripleTimestamp>>> keysToElementsInstances) {
        super(id, txn, clock);
        this.keysToElementsInstances = keysToElementsInstances;
    }

    @Override
    public Map<K, Set<V>> getValue() {
        Map<K, Set<V>> res = new HashMap<K, Set<V>>();
        for (Map.Entry<K, Map<V, Set<TripleTimestamp>>> entry : keysToElementsInstances.entrySet()) {
            Set<V> ps = entry.getValue().keySet();
            if (!ps.isEmpty()) {
                res.put(entry.getKey(), ps);
            }
        }
        return res;
    }

    // TODO put
    // TODO remove

    public void applyPut(K k, V e, TripleTimestamp uid, Set<TripleTimestamp> overwrittenInstances) {
        Map<V, Set<TripleTimestamp>> value = keysToElementsInstances.get(k);
        if (value == null) {
            keysToElementsInstances.put(k, value = new HashMap<V, Set<TripleTimestamp>>());
        }
        AddWinsUtils.applyAdd(value, e, uid, overwrittenInstances);
    }

    public void removeU(K k, V e, TripleTimestamp uid, Set<TripleTimestamp> rset) {
        Map<V, Set<TripleTimestamp>> value = keysToElementsInstances.get(k);
        if (value == null) {
            keysToElementsInstances.put(k, value = new HashMap<V, Set<TripleTimestamp>>());
        }
        AddWinsUtils.applyRemove(value, e, rset);
    }

    @Override
    public MapCRDT<K, V> copy() {
        return new MapCRDT(id, txn, clock, keysToElementsInstances);
    }

}