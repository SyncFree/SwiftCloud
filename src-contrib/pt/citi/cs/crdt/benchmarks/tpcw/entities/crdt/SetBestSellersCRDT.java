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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.BestSellerEntry;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.AbstractAddWinsSetCRDT;
import swift.crdt.AddWinsUtils;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class SetBestSellersCRDT extends AbstractAddWinsSetCRDT<BestSellerEntry, SetBestSellersCRDT> {
    private Map<BestSellerEntry, Set<TripleTimestamp>> elems;
    private static final int MAX_ITEMS = 50;

    // Kryo
    public SetBestSellersCRDT() {
    }

    public SetBestSellersCRDT(CRDTIdentifier id) {
        super(id, null, null);
        this.elems = new HashMap<BestSellerEntry, Set<TripleTimestamp>>();
    }

    private SetBestSellersCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            Map<BestSellerEntry, Set<TripleTimestamp>> elems) {
        super(id, txn, clock);
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
    @Override
    public void add(BestSellerEntry e) {
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

        super.add(e);
    }

    @Override
    public SetBestSellersCRDT copy() {
        Map<BestSellerEntry, Set<TripleTimestamp>> newElems = new HashMap<BestSellerEntry, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elems, newElems);
        return new SetBestSellersCRDT(id, txn, clock, newElems);
    }

    @Override
    protected Map<BestSellerEntry, Set<TripleTimestamp>> getElementsInstances() {
        return elems;
    }
}
