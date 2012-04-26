package swift.crdt;

import java.io.Serializable;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class RegisterVersioned<V extends Copyable> extends BaseCRDT<RegisterVersioned<V>> {
    private static final long serialVersionUID = 1L;

    private static class QueueEntry<V extends Copyable> implements Comparable<QueueEntry<V>>, Serializable {
        TripleTimestamp ts;
        CausalityClock c;
        V value;

        /**
         * Only to be used by Kryo serialization.
         */
        public QueueEntry() {
        }

        public QueueEntry(TripleTimestamp ts, CausalityClock c, V value) {
            this.ts = ts;
            this.c = c;
            this.value = value;
        }

        @Override
        public int compareTo(QueueEntry<V> other) {
            CMP_CLOCK result = this.c.compareTo(other.c);
            switch (result) {
            case CMP_CONCURRENT:
            case CMP_EQUALS:
                if (other.ts == null) {
                    return 1;
                } else {
                    return other.ts.compareTo(this.ts);
                }
            case CMP_ISDOMINATED:
                return 1;
            case CMP_DOMINATES:
                return -1;
            default:
                return 0;
            }
        }

        @Override
        public String toString() {
            return value + " -> " + ts + "," + c;
        }

        public QueueEntry<V> copy() {
            QueueEntry<V> copyObj = new QueueEntry<V>(ts, c.clone(), (V) value.copy());
            return copyObj;
        }
    }

    // Queue holding the versioning information, ordering is compatible with
    // causal dependency, newest entries coming first
    private SortedSet<QueueEntry<V>> values;

    public RegisterVersioned() {
        this.values = new TreeSet<QueueEntry<V>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        Iterator<QueueEntry<V>> it = values.iterator();
        while (it.hasNext()) {
            QueueEntry<V> entry = it.next();
            if (!entry.c.includes(ts)) {
                break;
            }
            if (entry.ts.equals(ts)) {
                it.remove();
            }
        }
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        // at least one value should remain after pruning
        if (values.size() == 1) {
            return;
        }
        // remove all versions older than the pruningPoint
        SortedSet<QueueEntry<V>> pruned = new TreeSet<QueueEntry<V>>();
        for (QueueEntry<V> e : values) {
            if (!pruningPoint.includes(e.ts)) {
                pruned.add(e);
            }
        }
        if (pruned.isEmpty()) {
            pruned = new TreeSet<QueueEntry<V>>();
            pruned.add(values.first());
        }
        values = pruned;
    }

    public void update(V val, TripleTimestamp ts, CausalityClock c) {
        values.add(new QueueEntry<V>(ts, c, val));
    }

    @Override
    protected void mergePayload(RegisterVersioned<V> otherObject) {
        CMP_CLOCK cmpClock = otherObject.getPruneClock().compareTo(getPruneClock());
        if (cmpClock == CMP_CLOCK.CMP_DOMINATES) {
            pruneImpl(otherObject.getPruneClock());
        } else {
            if (cmpClock == CMP_CLOCK.CMP_ISDOMINATED) {
                otherObject.pruneImpl(getPruneClock());
            }
        }
        values.addAll(otherObject.values);
    }

    @Override
    protected TxnLocalCRDT<RegisterVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final RegisterVersioned<V> creationState = isRegisteredInStore() ? null : new RegisterVersioned<V>();
        RegisterTxnLocal<V> localview = new RegisterTxnLocal<V>(id, txn, versionClock, creationState,
                value(versionClock));
        return localview;
    }

    private V value(CausalityClock versionClock) {
        for (QueueEntry<V> e : values) {
            if (versionClock.getLatest(e.ts.getIdentifier()).includes(e.ts)) {
                return e.value;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    protected void execute(CRDTOperation<RegisterVersioned<V>> op) {
        op.applyTo(this);
    }

    @Override
    public RegisterVersioned<V> copy() {
        RegisterVersioned<V> copyObj = new RegisterVersioned<V>();
        for (QueueEntry<V> e : values) {
            copyObj.values.add(e.copy());
        }
        copyObj.init(id, getClock().clone(), getPruneClock().clone(), registeredInStore);
        return copyObj;
    }

    @Override
    protected boolean hasUpdatesSinceImpl(CausalityClock clock) {
        for (QueueEntry<V> e : values) {
            if (!clock.includes(e.ts)) {
                return true;
            }
        }
        return false;
    }
}
