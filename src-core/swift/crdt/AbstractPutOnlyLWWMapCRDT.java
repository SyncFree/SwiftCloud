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
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * CRDT put-only map with Last Writer Wins resolution policy for concurrent
 * puts. WARNING: When putting an object to a map, make sure that both the and
 * the value are either immutable or that they are cloned! Keys must provide a
 * hashcode, since the internal representation uses HashMap.
 * 
 * @param <T>
 *            concrete implementation type for some type of K and V
 * @param <K>
 *            type of the key of a map
 * @param <V>
 *            type of the value of a map
 * @author mzawirsk
 */
public abstract class AbstractPutOnlyLWWMapCRDT<K, V, T extends AbstractPutOnlyLWWMapCRDT<K, V, T>> extends BaseCRDT<T> {
    public static class LWWEntry<V> {
        protected V val;
        protected long timestamp;
        protected TripleTimestamp timestampTiebreaker;

        protected void applySet(final long timestamp, final TripleTimestamp timestampTiebreaker, final V value) {
            if (timestamp < this.timestamp
                    || (timestamp == this.timestamp && timestampTiebreaker.compareTo(this.timestampTiebreaker) <= 0)) {
                // Last Writer Wins

                return;
            }
            // if (timestamp == this.timestamp &&
            // timestampTiebreaker.equals(this.timestampTiebreaker)) {
            // that should not happen under reliable delivery
            // }
            this.val = value;
            this.timestamp = timestamp;
            this.timestampTiebreaker = timestampTiebreaker;
        }
    }

    protected HashMap<K, LWWEntry<V>> entries;

    // Kryo
    public AbstractPutOnlyLWWMapCRDT() {
    }

    public AbstractPutOnlyLWWMapCRDT(CRDTIdentifier uid) {
        super(uid);
        // default values are just right
        entries = new HashMap<K, LWWEntry<V>>();
    }

    protected AbstractPutOnlyLWWMapCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            HashMap<K, LWWEntry<V>> entries) {
        super(id, txn, clock);
        this.entries = entries;
    }

    public void put(K key, V value) {
        TripleTimestamp ts = nextTimestamp();
        LWWEntry<V> entry = entries.get(key);
        if (entry == null) {
            entry = new LWWEntry<V>();
            entries.put(key, entry);
        }

        entry.timestamp++;
        entry.timestampTiebreaker = ts;
        entry.val = value;
        registerLocalOperation(generatePutDownstream(key, entry));
    }

    protected PutOnlyLWWMapUpdate<K, V, T> generatePutDownstream(K key, LWWEntry<V> entry) {
        return new PutOnlyLWWMapUpdate<K, V, T>(key, entry.timestamp, entry.timestampTiebreaker, entry.val);
    }

    public boolean contains(K key) {
        return entries.containsKey(key);
    }

    public V get(K key) {
        final LWWEntry<V> entry = entries.get(key);
        if (entry == null) {
            return null;
        }
        return entry.val;
    }

    @Override
    public HashMap<K, V> getValue() {
        final HashMap<K, V> result = new HashMap<K, V>();
        for (final Entry<K, LWWEntry<V>> entry : entries.entrySet()) {
            result.put(entry.getKey(), entry.getValue().val);
        }
        return result;
    }

    protected void applyPut(final K key, final long timestamp, final TripleTimestamp timestampTiebreaker, final V value) {
        LWWEntry<V> entry = entries.get(key);
        if (entry == null) {
            entry = new LWWEntry<V>();
            entry.timestamp = timestamp;
            entry.timestampTiebreaker = timestampTiebreaker;
            entry.val = value;
            entries.put(key, entry);
        } else {
            entry.applySet(timestamp, timestampTiebreaker, value);
        }
    }
}
