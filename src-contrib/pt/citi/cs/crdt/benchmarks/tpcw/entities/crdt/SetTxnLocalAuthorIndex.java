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
package pt.citi.cs.crdt.benchmarks.tpcw.entities.crdt;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.AuthorIndex;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDTTxnLocal;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalAuthorIndex extends BaseCRDTTxnLocal<SetAuthorIndex> {
	private Map<AuthorIndex, Set<TripleTimestamp>> elems;
	private PriorityQueue<AuthorIndex> cachedIndex;
	private final static int maxSize = 50;

	public SetTxnLocalAuthorIndex(CRDTIdentifier id, TxnHandle txn,
			CausalityClock clock, SetAuthorIndex creationState,
			Map<AuthorIndex, Set<TripleTimestamp>> elems) {
		super(id, txn, clock, creationState);
		this.elems = elems;
	}

	/**
	 * Insert element e in the set, using the given unique identifier.
	 * 
	 * @param e
	 */
	public void insert(AuthorIndex e) {
		if(elems.size() >= maxSize && cachedIndex == null){
			getOrderedValues();
			elems.remove(cachedIndex.peek());
		}
		Set<TripleTimestamp> adds = elems.get(e);
		if (adds == null) {
			adds = new HashSet<TripleTimestamp>();
			elems.put(e, adds);
		}
		cachedIndex = null;
		TripleTimestamp ts = nextTimestamp();
		adds.add(ts);
		registerLocalOperation(new SetInsert<AuthorIndex, SetAuthorIndex>(ts, e));
	}

	/**
	 * Remove element e from the set.
	 * 
	 * @param e
	 */
	public void remove(AuthorIndex e) {
		cachedIndex = null;
		Set<TripleTimestamp> ids = elems.remove(e);
		if (ids != null) {
			TripleTimestamp ts = nextTimestamp();
			registerLocalOperation(new SetRemove<AuthorIndex, SetAuthorIndex>(
					ts, e, ids));
		}
	}

	public boolean lookup(SCLine e) {
		return elems.containsKey(e);
	}

	public Set<AuthorIndex> getValue() {
		return elems.keySet();
	}

	@Override
	public Object executeQuery(CRDTQuery<SetAuthorIndex> query) {
		return query.executeAt(this);
	}

	public Collection<AuthorIndex> getOrderedValues() {
		if (cachedIndex != null)
			return cachedIndex;

		if (elems.size() == 0)
			return new LinkedList<AuthorIndex>();

		cachedIndex = new PriorityQueue<AuthorIndex>(elems.size(), new Comparator<AuthorIndex>() {

			@Override
			public int compare(AuthorIndex o1, AuthorIndex o2) {
				return o1.getDate().compareTo(o2.getDate());
			}
		});
		for (Entry<AuthorIndex, Set<TripleTimestamp>> e : elems.entrySet())
			cachedIndex.add(e.getKey());
		return cachedIndex;
	}
}
