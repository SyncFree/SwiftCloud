package swift.crdt.operations;

import swift.clocks.TripleTimestamp;
import swift.crdt.AbstractSortedSetVersioned;

public class SortedSetInsert<V extends Comparable<V>, T extends AbstractSortedSetVersioned<V, T>> extends BaseUpdate<T> {
    private V val;

    // required for kryo
    SortedSetInsert() {
    }

    public SortedSetInsert(TripleTimestamp ts, V val) {
        super(ts);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.insertU(val, getTimestamp());
    }
}
