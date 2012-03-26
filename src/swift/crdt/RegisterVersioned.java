package swift.crdt;

import java.util.PriorityQueue;

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

        @Override
        public int compareTo(QueueEntry<V> other) {
            CMP_CLOCK result = this.c.compareTo(other.c);
            switch (result) {
            case CMP_CONCURRENT:
            case CMP_EQUALS:
                return this.ts.compareTo(other.ts);
            case CMP_ISDOMINATED:
                return -1;
            case CMP_DOMINATES:
                return 1;
            }
            return 0;
        }

    }

    private PriorityQueue<QueueEntry<V>> values;

    public RegisterVersioned() {
        this.values = new PriorityQueue<QueueEntry<V>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void executeImpl(CRDTOperation<RegisterVersioned<V>> op) {
        op.applyTo(this);
    }

    public void update(V val, TripleTimestamp ts) {
        // TODO
    }

    @Override
    protected void mergePayload(RegisterVersioned<V> otherObject) {
        // TODO Auto-generated method stub

    }

    @Override
    protected TxnLocalCRDT<RegisterVersioned<V>> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        // TODO Auto-generated method stub
        return null;
    }

}
