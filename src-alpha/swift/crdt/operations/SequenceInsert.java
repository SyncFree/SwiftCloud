package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.SortedSetVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class SequenceInsert<V extends Comparable<V>, T extends SortedSetVersioned<V, T>> extends BaseUpdate<T> {
    private V val;

    // required for kryo
    protected SequenceInsert() {
    }

    public SequenceInsert(TripleTimestamp ts, V val) {
        super(ts);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        // Insert does not rely on any timestamp dependency.
    }

    @Override
    public void applyTo(T crdt) {
        crdt.insertU(val, getTimestamp());
    }

    @Override
    public CRDTUpdate<T> withBaseTimestamp(Timestamp ts) {
        return new SequenceInsert<V, T>(getTimestamp().withBaseTimestamp(ts), val);
    }
}
