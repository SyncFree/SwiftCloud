package swift.crdt.operations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.SetVersioned;
import swift.crdt.interfaces.CRDTOperation;

public class SetRemove<V, T extends SetVersioned<V, T>> extends BaseOperation<T> {
    private V val;
    private Set<TripleTimestamp> ids;

    // required for kryo
    public SetRemove() {
    }

    public SetRemove(TripleTimestamp ts, V val, Set<TripleTimestamp> ids) {
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
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
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
    public CRDTOperation<T> withBaseTimestamp(Timestamp ts) {
        return new SetRemove<V, T>(getTimestamp().withBaseTimestamp(ts), val, ids);
    }
}
