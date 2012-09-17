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
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

@SuppressWarnings("serial")
public class DirectoryVersioned extends BaseCRDT<DirectoryVersioned> {
    private Map<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>> dir;

    public DirectoryVersioned() {
        this.dir = new HashMap<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>>();
    }

    @Override
    public void rollback(Timestamp ts) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        Iterator<Entry<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>>> entries = dir.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>> e = entries.next();
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
    protected void mergePayload(DirectoryVersioned other) {
        Iterator<Entry<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>>> it = other.dir.entrySet()
                .iterator();
        while (it.hasNext()) {
            Entry<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>> e = it.next();

            Map<TripleTimestamp, Set<TripleTimestamp>> s = dir.get(e.getKey());
            if (s == null) {
                Map<TripleTimestamp, Set<TripleTimestamp>> newSet = new HashMap<TripleTimestamp, Set<TripleTimestamp>>(
                        e.getValue());
                dir.put(e.getKey(), newSet);

            } else {
                for (Entry<TripleTimestamp, Set<TripleTimestamp>> otherE : e.getValue().entrySet()) {
                    boolean exists = false;
                    for (Entry<TripleTimestamp, Set<TripleTimestamp>> localE : s.entrySet()) {
                        if (localE.getKey().equals(otherE.getKey())) {
                            localE.getValue().addAll(otherE.getValue());
                            exists = true;
                        }
                    }
                    if (!exists) {
                        s.put(otherE.getKey(), otherE.getValue());
                    }
                }
            }
        }
    }

    protected void execute(CRDTUpdate<DirectoryVersioned> op) {
        op.applyTo(this);
    }

    public void applyPut(CRDTIdentifier entry, TripleTimestamp uid) {
        Map<TripleTimestamp, Set<TripleTimestamp>> meta = dir.get(entry);
        if (meta == null) {
            meta = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            meta.put(uid, new HashSet<TripleTimestamp>());
            dir.put(entry, meta);
        }
    }

    public void applyRemove(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp uid) {
        Map<TripleTimestamp, Set<TripleTimestamp>> s = dir.get(key);
        if (s == null) {
            s = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            dir.put(key, s);
        }
        for (TripleTimestamp ts : toBeRemoved) {
            Set<TripleTimestamp> removals = s.get(ts);
            if (removals == null) {
                removals = new HashSet<TripleTimestamp>();
            }
            removals.add(uid);
        }
    }

    @Override
    protected TxnLocalCRDT<DirectoryVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        Map<CRDTIdentifier, Set<TripleTimestamp>> payload = new HashMap<CRDTIdentifier, Set<TripleTimestamp>>();

        // generate payload for versionClock
        Set<Entry<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>>> entrySet = dir.entrySet();
        for (Entry<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>> e : entrySet) {

            Set<TripleTimestamp> present = new HashSet<TripleTimestamp>();
            for (Entry<TripleTimestamp, Set<TripleTimestamp>> meta : e.getValue().entrySet()) {
                if (versionClock.includes(meta.getKey())) {
                    boolean add = true;
                    for (TripleTimestamp remTs : meta.getValue()) {
                        if (versionClock.includes(remTs)) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        present.add(meta.getKey());
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
        final Set<Timestamp> result = new HashSet<Timestamp>();
        for (Map<TripleTimestamp, Set<TripleTimestamp>> addsRemoves : dir.values()) {
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

}
