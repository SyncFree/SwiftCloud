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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import pt.citi.cs.crdt.benchmarks.tpcw.database.TPCW_SwiftCloud_Executor;
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

public class SetIndexByDateCRDT extends AbstractAddWinsSetCRDT<OrderInfo, SetIndexByDateCRDT> {
    private Map<OrderInfo, Set<TripleTimestamp>> elems;
    private static final int maxItems = TPCW_SwiftCloud_Executor.NUM_ORDERS_ADMIN;

    // Kryo
    public SetIndexByDateCRDT() {
    }

    public SetIndexByDateCRDT(final CRDTIdentifier id) {
        super(id, null, null);
        this.elems = new HashMap<OrderInfo, Set<TripleTimestamp>>();
    }

    private SetIndexByDateCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            Map<OrderInfo, Set<TripleTimestamp>> elems) {
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
    public void add(OrderInfo e) {
        if (elems.size() >= maxItems) {
            OrderInfo minValue = e;
            for (Entry<OrderInfo, Set<TripleTimestamp>> entry : elems.entrySet()) {
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

        super.add(e);
    }

    @Override
    public SetIndexByDateCRDT copy() {
        final HashMap<OrderInfo, Set<TripleTimestamp>> newInstances = new HashMap<OrderInfo, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elems, newInstances);
        return new SetIndexByDateCRDT(id, txn, clock, newInstances);
    }

    @Override
    protected Map<OrderInfo, Set<TripleTimestamp>> getElementsInstances() {
        return elems;
    }
}
