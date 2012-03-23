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
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;
import swift.exceptions.NotSupportedOperationException;
import swift.utils.Pair;
import swift.utils.PrettyPrint;

/**
 * CRDT SET with versioning support
 * 
 * @author vb, annettebieniusa
 * 
 * @param <V>
 */
public abstract class SetVersioned<V, T extends SetVersioned<V, T>> extends BaseCRDT<T> {

    private static final long serialVersionUID = 1L;
    private Map<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> elems;

    public SetVersioned() {
        elems = new HashMap<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>();
    }

    public Map<V, Set<TripleTimestamp>> getValue(CausalityClock snapshotClock) {
        Map<V, Set<TripleTimestamp>> retValues = new HashMap<V, Set<TripleTimestamp>>();

        Set<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> entrySet = elems.entrySet();
        for (Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e : entrySet) {
            Set<TripleTimestamp> present = new HashSet<TripleTimestamp>();
            for (Pair<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue()) {
                if (snapshotClock.includes(p.getFirst())) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getSecond()) {
                        if (snapshotClock.includes(remTs)) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        present.add(p.getFirst());
                    }
                }
            }

            if (present != null) {
                retValues.put(e.getKey(), present);
            }
        }
        return retValues;
    }

    private void insertU(V e, TripleTimestamp uid) {
        Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> entry = elems.get(e);
        // if element not present in the set, add entry for it in payload
        if (entry == null) {
            entry = new HashSet<Pair<TripleTimestamp, Set<TripleTimestamp>>>();
            elems.put(e, entry);
        }
        Pair<TripleTimestamp, Set<TripleTimestamp>> newValue = new Pair<TripleTimestamp, Set<TripleTimestamp>>(uid,
                new HashSet<TripleTimestamp>());
        entry.add(newValue);
    }

    private void removeU(V e, TripleTimestamp uid) {
        Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> s = elems.get(e);
        if (s == null) {
            return;
        }
        for (Pair<TripleTimestamp, Set<TripleTimestamp>> p : s) {
            p.getSecond().add(uid);
        }

    }

    @Override
    protected void mergePayload(T other) {
        Iterator<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> it = other.elems.entrySet().iterator();
        while (it.hasNext()) {
            Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e = it.next();
            Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> s = elems.get(e.getKey());
            if (s == null) {
                Set<Pair<TripleTimestamp, Set<TripleTimestamp>>> newSet = new HashSet<Pair<TripleTimestamp, Set<TripleTimestamp>>>(
                        e.getValue());
                elems.put(e.getKey(), newSet);
            } else {
                for (Pair<TripleTimestamp, Set<TripleTimestamp>> otherE : e.getValue()) {
                    boolean exists = false;
                    for (Pair<TripleTimestamp, Set<TripleTimestamp>> localE : s) {
                        if (localE.getFirst().equals(otherE.getFirst())) {
                            localE.getSecond().addAll(otherE.getSecond());
                            exists = true;
                        }
                    }
                    if (!exists) {
                        Pair<TripleTimestamp, Set<TripleTimestamp>> newPair = new Pair<TripleTimestamp, Set<TripleTimestamp>>(
                                otherE.getFirst(), otherE.getSecond());
                        s.add(newPair);
                    }
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
        Iterator<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> entries = elems.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e = entries.next();
            Iterator<Pair<TripleTimestamp, Set<TripleTimestamp>>> perClient = e.getValue().iterator();
            while (perClient.hasNext()) {
                Pair<TripleTimestamp, Set<TripleTimestamp>> valueTS = perClient.next();
                if (valueTS.getFirst().equals(rollbackEvent)) {
                    perClient.remove();
                } else {
                    Iterator<TripleTimestamp> remTS = valueTS.getSecond().iterator();
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
        // TODO
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void executeImpl(CRDTOperation op) {
        try {
            if (op instanceof SetInsert) {
                SetInsert<?> addop = (SetInsert<?>) op;
                this.insertU((V) addop.getVal(), addop.getTimestamp());
            } else if (op instanceof SetRemove) {
                SetRemove<?> subop = (SetRemove<?>) op;
                this.removeU((V) subop.getVal(), subop.getTimestamp());
            } else {
                throw new NotSupportedOperationException("Operation " + op + " is not supported for CRDT " + this.id);
            }
        } catch (ClassCastException e) {
            throw new NotSupportedOperationException("Operation " + op + " is not supported for CRDT " + this.id
                    + ": Wrong type of elements");
        }
    }
}
