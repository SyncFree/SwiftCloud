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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.SequenceVersioned.PosID;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SequenceVersioned<V> extends AbstractSortedSetVersioned<PosID<V>, SequenceVersioned<V>> {
    private static final long serialVersionUID = 1L;

    public SequenceVersioned() {
    }

    @Override
    protected TxnLocalCRDT<SequenceVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final SequenceVersioned<V> creationState = isRegisteredInStore() ? null : new SequenceVersioned<V>();
        SequenceTxnLocal<V> localView = new SequenceTxnLocal<V>(id, txn, versionClock, creationState,
                getValue(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTUpdate<SequenceVersioned<V>> op) {
        op.applyTo(this);
    }

    @Override
    public SequenceVersioned<V> copy() {
        SequenceVersioned<V> copy = new SequenceVersioned<V>();
        copyLoad(copy);
        copyBase(copy);
        return copy;
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
}

class SID implements Comparable<SID> {
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
class SiteId {

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