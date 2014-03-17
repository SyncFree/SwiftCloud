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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import pt.citi.cs.crdt.benchmarks.tpcw.entities.AuthorIndex;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.AbstractAddWinsSetCRDT;
import swift.crdt.AddWinsUtils;
import swift.crdt.SetAddUpdate;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

public class SetAuthorIndexCRDT extends AbstractAddWinsSetCRDT<AuthorIndex, SetAuthorIndexCRDT> {
    private Map<AuthorIndex, Set<TripleTimestamp>> elems;
    transient private PriorityQueue<AuthorIndex> cachedIndex;
    private final static int maxSize = 50;

    // Kryo
    public SetAuthorIndexCRDT() {
    }

    public SetAuthorIndexCRDT(final CRDTIdentifier id) {
        super(id, null, null);
        elems = new HashMap<AuthorIndex, Set<TripleTimestamp>>();
    }

    private SetAuthorIndexCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            Map<AuthorIndex, Set<TripleTimestamp>> elems) {
        super(id, txn, clock);
        this.elems = elems;
    }

    public void add(AuthorIndex e) {
        if (elems.size() >= maxSize && cachedIndex == null) {
            getOrderedValues();
            elems.remove(cachedIndex.peek());
        }
        cachedIndex = null;
        TripleTimestamp ts = nextTimestamp();
        final Set<TripleTimestamp> overwrittenInstances = AddWinsUtils.add(elems, e, ts);
        registerLocalOperation(new SetAddUpdate<AuthorIndex, SetAuthorIndexCRDT>(e, ts, overwrittenInstances));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(AuthorIndex e) {
        cachedIndex = null;
        super.remove(e);
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

    @Override
    public SetAuthorIndexCRDT copy() {
        final HashMap<AuthorIndex, Set<TripleTimestamp>> newInstances = new HashMap<AuthorIndex, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(elems, newInstances);
        return new SetAuthorIndexCRDT(id, txn, clock, newInstances);
    }

    @Override
    protected Map<AuthorIndex, Set<TripleTimestamp>> getElementsInstances() {
        return elems;
    }
}
