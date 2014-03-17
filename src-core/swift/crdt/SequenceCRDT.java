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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

// WISHME: build it using 2PSet?
/**
 * Sequence CRDT.
 * 
 * @author smduarte
 * @param <V>
 *            content to store in the sequence
 */
public class SequenceCRDT<V> extends BaseCRDT<SequenceCRDT<V>> {
    protected SortedMap<PosID<V>, Set<TripleTimestamp>> setElems;
    protected transient List<PosID<V>> posIDs;
    protected transient List<V> atoms;

    // Kryo
    public SequenceCRDT() {
    }

    public SequenceCRDT(CRDTIdentifier id) {
        super(id, null, null);
        this.setElems = new TreeMap<PosID<V>, Set<TripleTimestamp>>();
    }

    private SequenceCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            SortedMap<PosID<V>, Set<TripleTimestamp>> elems) {
        super(id, txn, clock);
        this.setElems = elems;
    }

    /**
     * Inserts atom d into the position pos of the sequence
     */
    public void insertAt(int pos, V v) {
        PosID<V> posId = newPosId(pos, v);
        getOrComputeAtoms().add(pos, v);
        getOrComputePosIds().add(pos, posId);
        Set<TripleTimestamp> overwrittenInstances = AddWinsUtils.add(setElems, posId, posId.getTimestamp());
        // WISHME: could be optimized
        registerLocalOperation(new SequenceInsertUpdate(posId, posId.getTimestamp(), overwrittenInstances));
    }

    /**
     * Deletes atom at position pos
     */
    public V removeAt(int pos) {
        PosID<V> posId = getOrComputePosIds().remove(pos);
        V v = getOrComputeAtoms().remove(pos);
        Set<TripleTimestamp> removedInstances = AddWinsUtils.remove(setElems, posId);
        if (removedInstances != null) {
            registerLocalOperation(new SequenceRemoveUpdate(posId, removedInstances));
        }
        return v;
    }

    public int size() {
        return getOrComputeAtoms().size();
    }

    @Override
    public List<V> getValue() {
        return Collections.unmodifiableList(getOrComputeAtoms());
    }

    protected void applyRemove(PosID<V> posId, Set<TripleTimestamp> ids) {
        // Make sure to not affect existing view;
        // WISHME: could be done differently, if done consistently
        delinearize();
        AddWinsUtils.applyRemove(setElems, posId, ids);
    }

    protected void applyAdd(PosID<V> posId, TripleTimestamp id, Set<TripleTimestamp> overwrittenIds) {
        // Make sure to not affect existing view
        // WISHME: could be done differently, if done consistently
        delinearize();
        AddWinsUtils.applyAdd(setElems, posId, id, overwrittenIds);
    }

    private List<PosID<V>> getOrComputePosIds() {
        if (posIDs == null) {
            linearize();
        }
        return posIDs;
    }

    private List<V> getOrComputeAtoms() {
        if (atoms == null) {
            linearize();
        }
        return atoms;
    }

    private void linearize() {
        atoms = new ArrayList<V>();
        posIDs = new ArrayList<PosID<V>>();
        for (Map.Entry<PosID<V>, Set<TripleTimestamp>> i : setElems.entrySet()) {
            PosID<V> k = i.getKey();
            if (!k.isDeleted()) {
                getOrComputeAtoms().add(k.getAtom());
                getOrComputePosIds().add(k);
            }
        }
    }

    private void delinearize() {
        atoms = null;
        posIDs = null;
    }

    final private PosID<V> newPosId(int pos, V atom) {
        SID id = null;
        int size = size();

        final TripleTimestamp ts = nextTimestamp();
        if (pos == 0) {
            id = size == 0 ? SID.FIRST : SID.smallerThan(getOrComputePosIds().get(0).getId());
            return new PosID<V>(id, atom, ts);
        }
        if (pos == size) {
            id = SID.greaterThan(getOrComputePosIds().get(pos - 1).getId());
            return new PosID<V>(id, atom, ts);
        }

        PosID<V> lo = getOrComputePosIds().get(pos - 1), hi = getOrComputePosIds().get(pos);
        id = lo.getId().between(hi.getId());
        PosID<V> res = new PosID<V>(id, atom, ts);

        return res;
    }

    @Override
    public SequenceCRDT<V> copy() {
        final SortedMap<PosID<V>, Set<TripleTimestamp>> newSetElems = new TreeMap<PosID<V>, Set<TripleTimestamp>>();
        AddWinsUtils.deepCopy(setElems, newSetElems);
        return new SequenceCRDT<V>(id, txn, clock, newSetElems);
    }

    static class PosID<V> implements Comparable<PosID<V>> {

        V atom;
        SID id;
        TripleTimestamp timestamp;

        // for kryo

        PosID() {
        }

        PosID(SID id, V atom, TripleTimestamp ts) {
            this.id = id;
            this.atom = atom;
            this.timestamp = ts;
        }

        public SID getId() {
            return id;
        }

        public V getAtom() {
            return atom;
        }

        public TripleTimestamp getTimestamp() {
            return timestamp;
        }

        public boolean isDeleted() {
            return atom == null;
        }

        @Override
        public int compareTo(PosID<V> other) {
            int res = id.compareTo(other.id);
            return res != 0 ? res : timestamp.compareTo(other.timestamp);
        }

        public int hashCode() {
            return id.hashCode() ^ timestamp.hashCode();
        }

        public boolean equals(Object o) {
            PosID<V> other = (PosID<V>) o;
            return compareTo(other) == 0;
        }

        public PosID<V> deletedPosID() {
            return new PosID<V>(id, null, timestamp);
        }

        public String toString() {
            return String.format("<%s>", id, timestamp);
        }
    }

    static class SID implements Comparable<SID> {
        static final int TOP = 1 << 30;

        static final int INCREMENT = 1 << 9;
        static final Random rg = new Random(1L);
        static SID FIRST = new SID(new int[] { increment(0), SiteId.get() });

        int[] coords;

        // for kryo
        SID() {
        }

        protected SID(int[] coords) {
            this.coords = coords;
        }

        public SID between(SID other) {
            return new SID(between(this, other));
        }

        // TODO deal with underoverflow of first coordinate...
        static public SID smallerThan(SID x) {
            return new SID(new int[] { decrement(x.coord(0)), SiteId.get() });
        }

        // TODO deal with overflow of first coordinate...
        static public SID greaterThan(SID x) {
            return new SID(new int[] { increment(x.coord(0)), SiteId.get() });
        }

        @Override
        public int compareTo(SID other) {
            int dims = Math.max(dims(), other.dims());
            for (int i = 0; i < dims; i++) {
                int signum = coord(i) - other.coord(i);
                if (signum != 0)
                    return signum;
            }
            return 0;
        }

        final int coord(int d) {
            return d < coords.length ? coords[d] : 0;
        }

        static private int[] between(SID lo, SID hi) {

            int dims = Math.max(lo.dims(), hi.dims());

            int[] inc = Arrays.copyOf(lo.coords, dims);
            int[] res = Arrays.copyOf(lo.coords, dims);

            for (int i = 0; i < dims - 1; i += 2) {
                inc[i] = Math.min(2 * INCREMENT, ((hi.coord(i) - lo.coord(i)) + TOP) % TOP);
                res[i] = (lo.coord(i) + (inc[i] >> 1));
                if (res[i] > TOP) {
                    res[i] %= TOP;
                    res[i - 2] += 1;
                }
            }

            for (int i = 0; i < dims - 1; i += 2) {
                if (inc[i] > 1) {
                    res[i + 1] = SiteId.get();
                    res = Arrays.copyOf(res, i + 2);
                    return res;
                }
            }

            // we got a collision, increase #dims by 2.
            res = Arrays.copyOf(res, dims + 2);
            res[dims] = INCREMENT;
            res[dims + 1] = SiteId.get();

            return res;
        }

        static List<Integer> INT(int[] a) {
            Integer[] res = new Integer[a.length / 2];
            int j = 0;
            for (int i = 0; i < a.length; i++) {
                if ((i & 1) == 0)
                    res[j++] = a[i];
            }

            return Arrays.asList(res);
        }

        public int hashCode() {
            int res = 0;

            for (int i : coords)
                res ^= i;

            return res;
        }

        public boolean equals(Object other) {
            return (other instanceof SID) && compareTo((SID) other) == 0;
        }

        public String toString() {
            List<Integer> tmp = new ArrayList<Integer>();
            int j = 0;
            for (int i : coords)
                if ((j++ & 1) == 0)
                    tmp.add(i);

            return String.format("%s", tmp);
        }

        static private int increment(int base) {
            final int jitter = (INCREMENT >> 2) + rg.nextInt(INCREMENT >> 1);
            return base + jitter;
        }

        static private int decrement(int base) {
            final int jitter = (INCREMENT >> 2) + rg.nextInt(INCREMENT >> 1);
            return base - jitter;
        }

        final private int dims() {
            return coords.length;
        }
    }

    // TODO Needs to initialize SiteId from somewhere else...
    static class SiteId {

        private static final AtomicInteger uniqueId = new AtomicInteger(-1);

        static final ThreadLocal<Integer> uniqueNum = new ThreadLocal<Integer>() {
            @Override
            protected Integer initialValue() {
                return uniqueId.getAndIncrement();
            }
        };

        public static Integer get() {
            return uniqueId.get();
        }

    }
}
