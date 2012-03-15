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
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
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
public abstract class SetVersioned<V> extends BaseCRDT<SetVersioned<V>> {

    private static final long serialVersionUID = 1L;
    private Map<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> elems;

    public SetVersioned() {
        elems = new HashMap<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>();
    }

    public Set<V> getValue(CausalityClock snapshotClock) {
        Set<Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>>> entrySet = elems.entrySet();
        Set<V> retValues = new HashSet<V>();

        for (Entry<V, Set<Pair<TripleTimestamp, Set<TripleTimestamp>>>> e : entrySet) {
            boolean add = false;
            for (Pair<TripleTimestamp, Set<TripleTimestamp>> p : e.getValue()) {
                if (snapshotClock.includes(p.getFirst())) {
                    if (p.getSecond().isEmpty()) {
                        add = true;
                    } else {
                        add = true;
                        for (TripleTimestamp remTs : p.getSecond()) {
                            if (snapshotClock.includes(remTs)) {
                                add = false;
                                break;
                            }
                        }
                    }
                }
            }
            if (add) {
                retValues.add(e.getKey());
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
    protected void mergePayload(SetVersioned<V> other) {
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
        SetVersioned<?> that = (SetVersioned<?>) o;
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
    public void prune(CausalityClock pruningPoint) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void executeImpl(CRDTOperation op) {
        if (op instanceof SetInsert) {
            SetInsert<V> addop = (SetInsert<V>) op;
            this.insertU(addop.getVal(), addop.getTimestamp());
        } else if (op instanceof SetRemove) {
            SetRemove<V> subop = (SetRemove<V>) op;
            this.removeU(subop.getVal(), subop.getTimestamp());
        } else {
            throw new NotSupportedOperationException();
        }
    }

    @Override
    public <X extends TxnLocalCRDT<SetVersioned<V>>> X getTxnLocalCopy(CausalityClock pruneClock,
            CausalityClock versionClock, TxnHandle txn) {

        SetTxnLocal<V> localView = new SetTxnLocal<V>(id, txn, versionClock, getValue(versionClock));
        return (X) localView;
    }

}
