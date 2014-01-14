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
import sys.net.impl.KryoLib;

/**
 * CRDT map with versioning support. WARNING: When constructing txn-local
 * versions of maps, make sure that the elements in the map are either immutable
 * or that they are cloned!
 * 
 * @author vb, annettebieniusa, mzawirsk, smduarte
 * 
 * @param <V>
 */
public abstract class AbstractMapVersioned<K, V, T extends AbstractMapVersioned<K, V, T>> extends BaseCRDT<T> {

    private static final long serialVersionUID = 1L;
    protected Map<K, SetVersioned<V>> elems;

    public AbstractMapVersioned() {
        elems = new HashMap<K, SetVersioned<V>>();
    }

    public void putU(K k, V e, TripleTimestamp uid) {
        SetVersioned<V> set = elems.get(k);
        if (set == null) {
            elems.put(k, set = new SetVersioned<V>());
            set.init(this.getUID(), this.getClock(), this.getPruneClock(), false);
        }
        set.insertU(e, uid);
    }

    public void removeU(K k, V e, TripleTimestamp uid, Set<TripleTimestamp> rset) {
        SetVersioned<V> set = elems.get(k);
        if (set == null) {
            elems.put(k, set = new SetVersioned<V>());
            set.init(this.getUID(), this.getClock(), this.getPruneClock(), false);
        }
        set.removeU(e, uid, rset);
    }

    public Map<K, Set<V>> getValue(CausalityClock snapshotClock) {
        Map<K, Set<V>> res = new HashMap<K, Set<V>>();
        for (Map.Entry<K, SetVersioned<V>> i : elems.entrySet()) {
            Set<V> ps = i.getValue().getValue(snapshotClock).keySet();
            if (ps.size() > 0)
                res.put(i.getKey(), ps);
        }
        return res;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<K, V> getMergedValue(CausalityClock snapshotClock) {
        Map<K, V> res = new HashMap<K, V>();
        for (Map.Entry<K, SetVersioned<V>> i : elems.entrySet()) {
            V v = null;
            for (V j : i.getValue().getValue(snapshotClock).keySet()) {
                if (v == null)
                    v = KryoLib.copy(j);
                else {
                    ((BaseCRDT) v).merge((BaseCRDT) j);
                }
            }
            if (v != null)
                res.put(i.getKey(), v);
        }
        return res;
    }

    public Map<K, Set<V>> getValue() {
        return getValue(this.getClock());
    }
}