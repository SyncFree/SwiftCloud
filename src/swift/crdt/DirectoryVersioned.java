package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.utils.Pair;

@SuppressWarnings("serial")
public class DirectoryVersioned extends BaseCRDT<DirectoryVersioned> {
    private Map<String, Map<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>>> dir;

    public DirectoryVersioned() {
        this.dir = new HashMap<String, Map<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected void mergePayload(DirectoryVersioned otherObject) {
        throw new RuntimeException("Not implemented yet!");
    }

    protected void execute(CRDTUpdate<DirectoryVersioned> op) {
        op.applyTo(this);
    }

    public void applyPut(String key, CRDT<?> val, Set<TripleTimestamp> toBeRemoved, TripleTimestamp uid) {
        Map<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>> entry = dir.get(key);
        if (entry == null) {
            entry = new HashMap<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>>();
            dir.put(key, entry);
        }
        Pair<CRDT<?>, Set<TripleTimestamp>> removals = entry.get(uid);
        if (removals == null) {
            entry.put(uid, new Pair<CRDT<?>, Set<TripleTimestamp>>(val, new HashSet<TripleTimestamp>()));
        } else {
            // entry has been removed
            entry.put(uid, new Pair<CRDT<?>, Set<TripleTimestamp>>(val, removals.getSecond()));
        }
    }

    public void applyRemove(String key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp uid) {
        Map<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>> s = dir.get(key);
        if (s == null) {
            s = new HashMap<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>>();
            dir.put(key, s);
        }

        for (TripleTimestamp ts : toBeRemoved) {
            Pair<CRDT<?>, Set<TripleTimestamp>> removals = s.get(ts);
            if (removals == null) {
                Set<TripleTimestamp> removedUids = new HashSet<TripleTimestamp>();
                removedUids.add(uid);
                removals = new Pair<CRDT<?>, Set<TripleTimestamp>>(null, removedUids);
                s.put(ts, removals);
            } else {
                removals.getSecond().add(uid);
            }
        }
    }

    @Override
    protected TxnLocalCRDT<DirectoryVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        Map<String, Map<TripleTimestamp, CRDT<?>>> payload = new HashMap<String, Map<TripleTimestamp, CRDT<?>>>();

        // generate payload for versionClock
        Set<Entry<String, Map<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>>>> entrySet = dir.entrySet();
        for (Entry<String, Map<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>>> e : entrySet) {

            Map<TripleTimestamp, CRDT<?>> present = new HashMap<TripleTimestamp, CRDT<?>>();
            for (Entry<TripleTimestamp, Pair<CRDT<?>, Set<TripleTimestamp>>> p : e.getValue().entrySet()) {
                if (versionClock.includes(p.getKey())) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getValue().getSecond()) {
                        if (versionClock.includes(remTs)) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        present.put(p.getKey(), p.getValue().getFirst().copy());
                    }
                }
            }
            if (!present.isEmpty()) {
                payload.put(e.getKey(), present);
            }
        }

        // FIXME What is this creationState stuff???
        final DirectoryVersioned creationState = isRegisteredInStore() ? null : new DirectoryVersioned();
        DirectoryTxnLocal localView = new DirectoryTxnLocal(id, txn, versionClock, creationState, payload);
        return localView;
    }

    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        throw new RuntimeException("Not implemented yet!");
    }

}
