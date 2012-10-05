package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.utils.PrettyPrint;

/**
 * CRDT set with versioning support. WARNING: When constructing txn-local
 * versions of sets, make sure that the elements in the set are either immutable
 * or that they are cloned!
 * 
 * @author vb, annettebieniusa, mzawirsk
 * 
 * @param <V>
 */
public abstract class SetVersioned<V, T extends SetVersioned<V, T>> extends BaseCRDT<T> {

    private static final long serialVersionUID = 1L;
    private Map<V, Map<TripleTimestamp, Set<TripleTimestamp>>> elems;

    public SetVersioned() {
        elems = new HashMap<V, Map<TripleTimestamp, Set<TripleTimestamp>>>();
    }

    public Map<V, Set<TripleTimestamp>> getValue(CausalityClock snapshotClock) {
        return AddWinsUtils.getValue(this.elems, snapshotClock);
    }

    public void insertU(V e, TripleTimestamp uid) {
        Map<TripleTimestamp, Set<TripleTimestamp>> entry = elems.get(e);
        // if element not present in the set, add entry for it in payload
        if (entry == null) {
            entry = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            elems.put(e, entry);
        }
        entry.put(uid, new HashSet<TripleTimestamp>());
        registerTimestampUsage(uid);
    }

    public void removeU(V e, TripleTimestamp uid, Set<TripleTimestamp> set) {
        Map<TripleTimestamp, Set<TripleTimestamp>> s = elems.get(e);
        if (s == null) {
            return;
        }

        for (TripleTimestamp ts : set) {
            Set<TripleTimestamp> removals = s.get(ts);
            if (removals != null) {
                removals.add(uid);
                registerTimestampUsage(uid);
            }
            // else: element uid has been already removed&pruned
        }
    }

    @Override
    protected void mergePayload(T other) {
        final List<TripleTimestamp> newTimestampUsages = new LinkedList<TripleTimestamp>();
        final List<TripleTimestamp> releasedTimestampUsages = new LinkedList<TripleTimestamp>();
        AddWinsUtils.mergePayload(this.elems, this.getClock(), other.elems, other.getClock(), newTimestampUsages,
                releasedTimestampUsages);
        for (final TripleTimestamp ts : newTimestampUsages) {
            registerTimestampUsage(ts);
        }
        for (final TripleTimestamp ts : releasedTimestampUsages) {
            unregisterTimestampUsage(ts);
        }
    }

    @Override
    public String toString() {
        return PrettyPrint.printMap("{", "}", ";", "->", elems);
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        final List<TripleTimestamp> releasedTimestampUsages = AddWinsUtils.pruneImpl(this.elems, pruningPoint);
        for (final TripleTimestamp ts : releasedTimestampUsages) {
            unregisterTimestampUsage(ts);
        }
    }

    protected void copyLoad(SetVersioned<V, T> copy) {
        copy.elems = new HashMap<V, Map<TripleTimestamp, Set<TripleTimestamp>>>();
        for (final Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e : this.elems.entrySet()) {
            final V v = e.getKey();
            final Map<TripleTimestamp, Set<TripleTimestamp>> subMap = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> subEntry : e.getValue().entrySet()) {
                subMap.put(subEntry.getKey(), new HashSet<TripleTimestamp>(subEntry.getValue()));
            }
            copy.elems.put(v, subMap);
        }
    }
}
