package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.utils.PrettyPrint;

/**
 * CRDT set with versioning support. WARNING: When constructing txn-local
 * versions of sets, make sure that the elements in the set are either immutable
 * or that they are cloned!
 * 
 * @author vb, annettebieniusa
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
        Map<V, Set<TripleTimestamp>> retValues = new HashMap<V, Set<TripleTimestamp>>();
        Set<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entrySet = elems.entrySet();
        for (Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e : entrySet) {
            Set<TripleTimestamp> present = new HashSet<TripleTimestamp>();
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue().entrySet()) {
                if (snapshotClock.includes(p.getKey())) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getValue()) {
                        if (snapshotClock.includes(remTs)) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        present.add(p.getKey());
                    }
                }
            }

            if (!present.isEmpty()) {
                retValues.put(e.getKey(), present);
            }
        }
        return retValues;
    }

    public void insertU(V e, TripleTimestamp uid) {
        Map<TripleTimestamp, Set<TripleTimestamp>> entry = elems.get(e);
        // if element not present in the set, add entry for it in payload
        if (entry == null) {
            entry = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            elems.put(e, entry);
        }
        entry.put(uid, new HashSet<TripleTimestamp>());
    }

    public void removeU(V e, TripleTimestamp uid, Set<TripleTimestamp> set) {
        Map<TripleTimestamp, Set<TripleTimestamp>> s = elems.get(e);
        if (s == null) {
            return;
        }

        for (TripleTimestamp ts : set) {
            Set<TripleTimestamp> removals = s.get(ts);
            if (removals == null) {
                removals = new HashSet<TripleTimestamp>();
                removals.add(uid);
                s.put(ts, removals);

            } else {
                removals.add(uid);
            }
        }
    }

    @Override
    protected void mergePayload(T other) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> it = other.elems.entrySet().iterator();
        while (it.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = it.next();

            Map<TripleTimestamp, Set<TripleTimestamp>> s = elems.get(e.getKey());
            if (s == null) {
                Map<TripleTimestamp, Set<TripleTimestamp>> newSet = new HashMap<TripleTimestamp, Set<TripleTimestamp>>(
                        e.getValue());
                elems.put(e.getKey(), newSet);

            } else {
                for (Entry<TripleTimestamp, Set<TripleTimestamp>> otherE : e.getValue().entrySet()) {
                    boolean exists = false;
                    for (Entry<TripleTimestamp, Set<TripleTimestamp>> localE : s.entrySet()) {
                        if (localE.getKey().equals(otherE.getKey())) {
                            localE.getValue().addAll(otherE.getValue());
                            exists = true;
                        }
                    }
                    if (!exists && !getPruneClock().includes(otherE.getKey())) {
                        s.put(otherE.getKey(), new HashSet<TripleTimestamp>(otherE.getValue()));
                    }
                }
            }
        }

        // Remove elements removed&pruned at the incoming replica.
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> ourIt = elems.entrySet().iterator();
        while (ourIt.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> ourE = ourIt.next();
            Map<TripleTimestamp, Set<TripleTimestamp>> otherInstances = other.elems.get(ourE.getKey());
            if (otherInstances == null) {
                // Consider dummy empty map in this case.
                otherInstances = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            }
            final Iterator<Entry<TripleTimestamp, Set<TripleTimestamp>>> ourInstanceIter = ourE.getValue().entrySet()
                    .iterator();
            while (ourInstanceIter.hasNext()) {
                final Entry<TripleTimestamp, Set<TripleTimestamp>> ourInstance = ourInstanceIter.next();
                if (other.getPruneClock().includes(ourInstance.getKey())
                        && !otherInstances.containsKey(ourInstance.getKey())) {
                    ourInstanceIter.remove();
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SetVersioned)) {
            return false;
        }
        SetVersioned<?, ?> that = (SetVersioned<?, ?>) o;
        return that.elems.equals(this.elems);
    }

    @Override
    public String toString() {
        return PrettyPrint.printMap("{", "}", ";", "->", elems);

    }

    @Override
    public void rollback(Timestamp rollbackEvent) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entries = elems.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = entries.next();
            Iterator<Map.Entry<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().entrySet().iterator();
            while (perClient.hasNext()) {
                Entry<TripleTimestamp, Set<TripleTimestamp>> valueTS = perClient.next();
                if (valueTS.getKey().equals(rollbackEvent)) {
                    perClient.remove();
                } else {
                    Iterator<TripleTimestamp> remTS = valueTS.getValue().iterator();
                    while (remTS.hasNext()) {
                        if (remTS.next().equals(rollbackEvent)) {
                            remTS.remove();
                        }
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                entries.remove();
            }
        }
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        Iterator<Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>>> entries = elems.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Map<TripleTimestamp, Set<TripleTimestamp>>> e = entries.next();
            Iterator<Map.Entry<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().entrySet().iterator();
            while (perClient.hasNext()) {
                Map.Entry<TripleTimestamp, Set<TripleTimestamp>> current = perClient.next();
                Iterator<TripleTimestamp> removals = current.getValue().iterator();
                while (removals.hasNext()) {
                    TripleTimestamp ts = removals.next();
                    if (pruningPoint.includes(ts)) {
                        perClient.remove();
                        break;
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                entries.remove();
            }
        }
    }

    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        final Set<Timestamp> result = new HashSet<Timestamp>();
        for (Map<TripleTimestamp, Set<TripleTimestamp>> addsRemoves : elems.values()) {
            for (final Entry<TripleTimestamp, Set<TripleTimestamp>> addRemoves : addsRemoves.entrySet()) {
                if (!clock.includes(addRemoves.getKey())) {
                    result.add(addRemoves.getKey().cloneBaseTimestamp());
                }
                for (final TripleTimestamp removeTimestamp : addRemoves.getValue()) {
                    if (!clock.includes(removeTimestamp)) {
                        result.add(removeTimestamp.cloneBaseTimestamp());
                    }
                }
            }
        }
        return result;
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
