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

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

public class SetTxnLocalBestSellers extends BaseCRDTTxnLocal<SetBestSellers> {
    private Map<BestSellerEntry, Set<TripleTimestamp>> elems;
    private static final int MAX_ITEMS = 50;

    public SetTxnLocalBestSellers(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SetBestSellers creationState,
            Map<BestSellerEntry, Set<TripleTimestamp>> elems) {
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
    public void insert(BestSellerEntry e) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        if (elems.size() >= MAX_ITEMS) {
            BestSellerEntry minValue = new BestSellerEntry(e.getI_SUBJECT(), e.getI_ID(), Integer.MAX_VALUE);
            for (Entry<BestSellerEntry, Set<TripleTimestamp>> entry : elems.entrySet()) {
                if (entry.getKey().getI_TOTAL_SOLD() < minValue.getI_TOTAL_SOLD())
                    minValue = entry.getKey();
                if (entry.getKey().getI_ID() == e.getI_ID()) {
                    if (entry.getKey().getI_TOTAL_SOLD() < e.getI_TOTAL_SOLD()) {
                        minValue = entry.getKey();
                        break;
                    }
                    return;
                }
            }
            remove(minValue);
        }

        Set<TripleTimestamp> adds = elems.get(e);
        if (adds == null) {
            adds = new HashSet<TripleTimestamp>();
            elems.put(e, adds);
        }

        TripleTimestamp ts = nextTimestamp();
        adds.add(ts);
        registerLocalOperation(new SetInsert<BestSellerEntry, SetBestSellers>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    private void remove(BestSellerEntry e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        if (ids != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new SetRemove<BestSellerEntry, SetBestSellers>(ts, e, ids));
        }
    }

    public boolean lookup(BestSellerEntry e) {
        return elems.containsKey(e);
    }

    public Set<BestSellerEntry> getValue() {
        return elems.keySet();
    }

    @Override
    public Object executeQuery(CRDTQuery<SetBestSellers> query) {
        return query.executeAt(this);
    }
}
