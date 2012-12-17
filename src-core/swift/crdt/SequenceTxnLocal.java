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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.SequenceVersioned.PosID;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SequenceInsert;
import swift.crdt.operations.SequenceRemove;

public class SequenceTxnLocal<V> extends BaseCRDTTxnLocal<SequenceVersioned<V>> {

    private SortedMap<PosID<V>, Set<TripleTimestamp>> elems;
    private List<PosID<V>> posIDs = new ArrayList<PosID<V>>();
    private List<V> atoms = new ArrayList<V>();

    public SequenceTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SequenceVersioned<V> creationState,
            SortedMap<PosID<V>, Set<TripleTimestamp>> elems) {
        super(id, txn, clock, creationState);
        this.elems = elems;

        this.linearize();
    }

    /**
     * Inserts atom d into the position pos of the sequence
     */
    public void insertAt(int pos, V v) {
        PosID<V> posId = newPosId(pos, v);
        insert(posId);
        atoms.add(pos, v);
        posIDs.add(pos, posId);
    }

    /**
     * Deletes atom at position pos
     */
    public V removeAt(int pos) {
        PosID<V> posId = posIDs.remove(pos);
        V v = atoms.remove(pos);
        remove(posId);
        return v;
    }

    public int size() {
        return atoms.size();
    }

    @Override
    public Object executeQuery(CRDTQuery<SequenceVersioned<V>> query) {
        return query.executeAt(this);
    }

    @Override
    public List<V> getValue() {
        return Collections.unmodifiableList(atoms);
    }

    private void linearize() {
        atoms.clear();
        posIDs.clear();
        for (Map.Entry<PosID<V>, Set<TripleTimestamp>> i : elems.entrySet()) {
            PosID<V> k = i.getKey();
            if (!k.isDeleted()) {
                atoms.add(k.getAtom());
                posIDs.add(k);
            }
        }
    }

    final private PosID<V> newPosId(int pos, V atom) {
        SID id = null;
        int size = size();

        if (pos == 0) {
            id = size == 0 ? SID.FIRST : SID.smallerThan(posIDs.get(0).getId());
            return new PosID<V>(id, atom, nextTimestamp());
        }
        if (pos == size) {
            id = SID.greaterThan(posIDs.get(pos - 1).getId());
            return new PosID<V>(id, atom, nextTimestamp());
        }

        PosID<V> lo = posIDs.get(pos - 1), hi = posIDs.get(pos);
        id = lo.getId().between(hi.getId());
        PosID<V> res = new PosID<V>(id, atom, nextTimestamp());

        return res;
    }

    /**
     * Insert element e in the supporting sorted set, using the given unique
     * identifier.
     * 
     * @param e
     */
    private void insert(PosID<V> e) {
        Set<TripleTimestamp> adds = elems.get(e);
        if (adds == null) {
            adds = new HashSet<TripleTimestamp>();
            elems.put(e, adds);
        }
        adds.add(e.getTimestamp());
        registerLocalOperation(new SequenceInsert<PosID<V>, SequenceVersioned<V>>(e.getTimestamp(), e));
    }

    /**
     * Remove element e from the supporting sorted set.
     * 
     * @param e
     */
    public void remove(PosID<V> e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        if (ids != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new SequenceRemove<PosID<V>, SequenceVersioned<V>>(ts, e, ids));
        }
    }

}
