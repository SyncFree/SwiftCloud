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

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * An generic version of {@link AbstractPutOnlyLWWMapCRDT} for any provided key
 * and value type.
 * 
 * @see PutOnlyLWWStringMapCRDT for an optimized version
 * @author mzawirsk
 */
public class PutOnlyLWWMapCRDT<K, V> extends AbstractPutOnlyLWWMapCRDT<PutOnlyLWWMapCRDT<K, V>, K, V> {
    // Kryo
    public PutOnlyLWWMapCRDT() {
    }

    public PutOnlyLWWMapCRDT(CRDTIdentifier uid) {
        super(uid);
    }

    private PutOnlyLWWMapCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, HashMap<K, LWWEntry<V>> entries) {
        super(id, txn, clock, entries);
    }

    @Override
    protected AbstractPutOnlyLWWMapUpdate<PutOnlyLWWMapCRDT<K, V>, K, V> generatePutDownstream(K key, LWWEntry<V> entry) {
        return new AbstractPutOnlyLWWMapUpdate<>(key, entry.timestamp, entry.timestampTiebreaker, entry.val);
    }

    @Override
    public PutOnlyLWWMapCRDT<K, V> copy() {
        return new PutOnlyLWWMapCRDT<K, V>(id, txn, clock, new HashMap<K, LWWEntry<V>>(entries));
    }
}
