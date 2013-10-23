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

import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import pt.citi.cs.crdt.benchmarks.tpcw.database.TPCW_SwiftCloud_Executor;
import pt.citi.cs.crdt.benchmarks.tpcw.entities.BestSellerEntry;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDTTxnLocal;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class SetTxnLocalIndexByDate extends BaseCRDTTxnLocal<SetIndexByDate> {
	private Map<OrderInfo, Set<TripleTimestamp>> elems;
	private static final int maxItems = TPCW_SwiftCloud_Executor.NUM_ORDERS_ADMIN;

	public SetTxnLocalIndexByDate(CRDTIdentifier id, TxnHandle txn,
			CausalityClock clock, SetIndexByDate creationState,
			Map<OrderInfo, Set<TripleTimestamp>> elems) {
		super(id, txn, clock, creationState);
		this.elems = elems;
	}

	/**
	 * TODO: When amount is equal to another item must add because it could have
	 * been added concurrently. Insert element e in the set, using the given
	 * unique identifier.
	 * 
	 * @param e
	 * @throws NetworkException
	 * @throws VersionNotFoundException
	 * @throws NoSuchObjectException
	 * @throws WrongTypeException
	 */
	public void insert(OrderInfo e) throws WrongTypeException,
			NoSuchObjectException, VersionNotFoundException, NetworkException {
		if (elems.size() >= maxItems) {
			OrderInfo minValue = e;
			for (Entry<OrderInfo, Set<TripleTimestamp>> entry : elems
					.entrySet()) {
				try {
					if (entry.getKey().getO_DATE() - minValue.getO_DATE() < 0)
						minValue = entry.getKey();
				} catch (ParseException e1) {
					e1.printStackTrace();
				}
			}
			if (minValue != e)
				remove(minValue);
			else
				return;
		}

		Set<TripleTimestamp> adds = elems.get(e);
		if (adds == null) {
			adds = new HashSet<TripleTimestamp>();
			elems.put(e, adds);
		}

		TripleTimestamp ts = nextTimestamp();
		adds.add(ts);
		registerLocalOperation(new SetInsert<OrderInfo, SetIndexByDate>(ts, e));
	}

	/**
	 * Remove element e from the set.
	 * 
	 * @param e
	 */
	private void remove(OrderInfo e) {
		Set<TripleTimestamp> ids = elems.remove(e);
		if (ids != null) {
			TripleTimestamp ts = nextTimestamp();
			registerLocalOperation(new SetRemove<OrderInfo, SetIndexByDate>(ts,
					e, ids));
		}
	}

	public boolean lookup(BestSellerEntry e) {
		return elems.containsKey(e);
	}

	public Set<OrderInfo> getValue() {
		return elems.keySet();
	}

	@Override
	public Object executeQuery(CRDTQuery<SetIndexByDate> query) {
		return query.executeAt(this);
	}
}
