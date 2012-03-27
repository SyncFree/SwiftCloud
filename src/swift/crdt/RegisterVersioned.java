package swift.crdt;

import java.util.Iterator;
import java.util.TreeSet;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class RegisterVersioned<V> extends BaseCRDT<RegisterVersioned<V>> {
    private static class QueueEntry<V> implements Comparable<QueueEntry<V>> {
        TripleTimestamp ts;
        CausalityClock c;
        V value;

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
                    return this.ts.compareTo(other.ts);
                }
            case CMP_ISDOMINATED:
                return -1;
            case CMP_DOMINATES:
                return 1;
            }
            return 0;
        }

    }

    // queue holding the versioning information, ordering is compatible with
    // causal dependency, newest entries coming first
    private TreeSet<QueueEntry<V>> values;

    public RegisterVersioned() {
        this.values = new TreeSet<QueueEntry<V>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        Iterator<QueueEntry<V>> it = values.descendingIterator();
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
        QueueEntry<V> dummy = new QueueEntry<V>(null, pruningPoint, null);
        values.tailSet(dummy);
    }

    @Override
    protected void executeImpl(CRDTOperation<RegisterVersioned<V>> op) {
        op.applyTo(this);
    }

    public void update(V val, TripleTimestamp ts) {
        values.add(new QueueEntry<V>(ts, this.getClock(), val));
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
        // TODO Auto-generated method stub
        return null;
    }

}
