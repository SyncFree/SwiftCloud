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

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 * CRDT set with versioning support. WARNING: When constructing txn-local
 * versions of sets, make sure that the elements in the set are either immutable
 * or that they are cloned!
 * 
 * @author vb, annettebieniusa, mzawirsk
 * 
 * @param <V>
 */
public class MapVersioned<K, V> extends AbstractMapVersioned<K, V, MapVersioned<K, V>> {

    private static final long serialVersionUID = 1L;

    public MapVersioned() {
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        // TODO Auto-generated method stub

    }

    public V get(K key) {
        Set<V> set = super.getValue().get(key);
        if (set != null)
            for (V i : set)
                return i;

        return null;
    }

    @Override
    protected boolean mergePayload(MapVersioned<K, V> other) {
        for (Map.Entry<K, SetVersioned<V>> i : other.elems.entrySet()) {
            K key = i.getKey();
            SetVersioned<V> tv = elems.get(key), ov = i.getValue();
            if (tv == null)
                elems.put(key, ov);
            else
                tv.merge(ov);
        }
        return false;
    }

    @Override
    protected void execute(CRDTUpdate<MapVersioned<K, V>> op) {
        op.applyTo(this);
    }

    @Override
    protected TxnLocalCRDT<MapVersioned<K, V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        // TODO Auto-generated method stub
        return null;
    }

    public String toString() {
        return "" + elems;
    }
}
