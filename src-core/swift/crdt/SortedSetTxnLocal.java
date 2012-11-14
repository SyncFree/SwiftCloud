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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SortedSetInsert;
import swift.crdt.operations.SortedSetRemove;

public class SortedSetTxnLocal<V extends Comparable<V>> extends BaseCRDTTxnLocal<SortedSetVersioned<V>> {

	private SortedMap<V, Set<TripleTimestamp>> elems;

	public SortedSetTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SortedSetVersioned<V> creationState, SortedMap<V, Set<TripleTimestamp>> elems) {
		super(id, txn, clock, creationState);
		this.elems = elems;
	}

	@Override
	public Set<V> getValue() {
		return Collections.unmodifiableSet( elems.keySet() );
	}

	public int size() {
	    return elems.size();
	}
	/**
	 * Insert element e in the supporting sorted set, using the given unique
	 * identifier.
	 * 
	 * @param e
	 */
	public void insert(V v) {
		Set<TripleTimestamp> adds = elems.get( v );
		if (adds == null) {
			adds = new HashSet<TripleTimestamp>();
			elems.put(v, adds);
		}
		TripleTimestamp ts = nextTimestamp();
		adds.add( ts );
		registerLocalOperation(new SortedSetInsert<V, SortedSetVersioned<V>>( ts, v ));
	}

	/**
	 * Remove element e from the supporting sorted set.
	 * 
	 * @param e
	 */
	public V remove(V v) {
		Set<TripleTimestamp> ids = elems.remove(v);
		if (ids != null) {
			TripleTimestamp ts = nextTimestamp();
			registerLocalOperation(new SortedSetRemove<V, SortedSetVersioned<V>>( ts, v, ids));
	        return v;
		} else
		    return null;
	}

    @Override
    public Object executeQuery(CRDTQuery<SortedSetVersioned<V>> query) {
        return query.executeAt(this);
    }

  
	
}
