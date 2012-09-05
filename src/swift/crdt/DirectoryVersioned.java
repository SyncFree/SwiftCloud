package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.utils.Pair;

@SuppressWarnings("serial")
public class DirectoryVersioned extends BaseCRDT<DirectoryVersioned> {
    private Map<String, Map<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>>> dir;

    public DirectoryVersioned() {
        this.dir = new HashMap<String, Map<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>>>();
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

    public void applyPut(String key, CRDTIdentifier val, Set<TripleTimestamp> toBeRemoved, TripleTimestamp uid) {
        Map<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>> entry = dir.get(key);
        if (entry == null) {
            entry = new HashMap<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>>();
            dir.put(key, entry);
        }
        Pair<CRDTIdentifier, Set<TripleTimestamp>> removals = entry.get(uid);
        if (removals == null) {
            entry.put(uid, new Pair<CRDTIdentifier, Set<TripleTimestamp>>(val, new HashSet<TripleTimestamp>()));
        } else {
            // entry has been removed
            entry.put(uid, new Pair<CRDTIdentifier, Set<TripleTimestamp>>(val, removals.getSecond()));
        }
    }

    public void applyRemove(String key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp uid) {
        Map<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>> s = dir.get(key);
        if (s == null) {
            s = new HashMap<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>>();
            dir.put(key, s);
        }

        for (TripleTimestamp ts : toBeRemoved) {
            Pair<CRDTIdentifier, Set<TripleTimestamp>> removals = s.get(ts);
            if (removals == null) {
                Set<TripleTimestamp> removedUids = new HashSet<TripleTimestamp>();
                removedUids.add(uid);
                removals = new Pair<CRDTIdentifier, Set<TripleTimestamp>>(null, removedUids);
                s.put(ts, removals);
            } else {
                removals.getSecond().add(uid);
            }
        }
    }

    @Override
    protected TxnLocalCRDT<DirectoryVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        Map<String, Map<TripleTimestamp, CRDTIdentifier>> payload = new HashMap<String, Map<TripleTimestamp, CRDTIdentifier>>();

        // generate payload for versionClock
        Set<Entry<String, Map<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>>>> entrySet = dir.entrySet();
        for (Entry<String, Map<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>>> e : entrySet) {

            Map<TripleTimestamp, CRDTIdentifier> present = new HashMap<TripleTimestamp, CRDTIdentifier>();
            for (Entry<TripleTimestamp, Pair<CRDTIdentifier, Set<TripleTimestamp>>> p : e.getValue().entrySet()) {
                if (versionClock.includes(p.getKey())) {
                    boolean add = true;
                    for (TripleTimestamp remTs : p.getValue().getSecond()) {
                        if (versionClock.includes(remTs)) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        present.put(p.getKey(), p.getValue().getFirst());
                    }
                }
            }
            if (!present.isEmpty()) {
                payload.put(e.getKey(), present);
            }
        }

        final DirectoryVersioned creationState = isRegisteredInStore() ? null : new DirectoryVersioned();
        DirectoryTxnLocal localView = new DirectoryTxnLocal(id, txn, versionClock, creationState, payload);
        return localView;
    }

    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        throw new RuntimeException("Not implemented yet!");
    }

}
