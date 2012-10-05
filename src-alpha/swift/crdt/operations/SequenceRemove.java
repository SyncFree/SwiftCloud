package swift.crdt.operations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.SetVersioned;
import swift.crdt.SortedSetVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class SequenceRemove<V extends Comparable<V>, T extends SortedSetVersioned<V, T>> extends BaseUpdate<T> {

    private V val;
    private Set<TripleTimestamp> ids;

    // required for kryo
    protected SequenceRemove() {
    }

    public SequenceRemove(TripleTimestamp ts, V val, Set<TripleTimestamp> ids) {
        super(ts);
        this.val = val;
        this.ids = ids;
    }

    public V getVal() {
        return this.val;
    }

    public Set<TripleTimestamp> getIds() {
        return this.ids;
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        final List<TripleTimestamp> newIds = new LinkedList<TripleTimestamp>();
        final Iterator<TripleTimestamp> iter = ids.iterator();
        while (iter.hasNext()) {
            final TripleTimestamp id = iter.next();
            if (oldTs.includes(id)) {
                newIds.add(id.withBaseTimestamp(newTs));
                iter.remove();
            }
        }
        ids.addAll(newIds);
    }

    @Override
    public void applyTo(T crdt) {
        crdt.removeU(val, getTimestamp(), ids);
    }

    @Override
    public CRDTUpdate<T> withBaseTimestamp(Timestamp ts) {
        return new SequenceRemove<V, T>(getTimestamp().withBaseTimestamp(ts), val, ids);
    }
}
