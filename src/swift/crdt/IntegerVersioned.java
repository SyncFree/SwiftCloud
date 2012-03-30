package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.NotSupportedOperationException;
import swift.utils.Pair;

public class IntegerVersioned extends BaseCRDT<IntegerVersioned> {
    private static final long serialVersionUID = 1L;
    private Map<String, Set<Pair<Integer, TripleTimestamp>>> updates;
    // Current value with respect to the updatesClock
    private int currentValue;

    // Value with respect to the pruneClock
    private Map<String, Pair<Integer, TripleTimestamp>> pruneVector;
    private int pruneValue;

    public IntegerVersioned() {
        this.updates = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
        this.pruneVector = new HashMap<String, Pair<Integer, TripleTimestamp>>();
    }

    private int value(CausalityClock snapshotClock) {
        if (snapshotClock.compareTo(getClock()) != CMP_CLOCK.CMP_ISDOMINATED) {
            // Since snapshot covers all updates making up this object, use the
            // current value.
            return currentValue;
        }
        int retValue = pruneValue;
        retValue += filterUpdates(snapshotClock);
        return retValue;
    }

    private int filterUpdates(CausalityClock clk) {
        int retValue = 0;
        for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> entry : updates.entrySet()) {
            for (Pair<Integer, TripleTimestamp> set : entry.getValue()) {
                if (clk.includes(set.getSecond())) {
                    retValue += set.getFirst();
                }
            }
        }
        return retValue;
    }

    public void applyUpdate(int n, TripleTimestamp ts) {
        String siteId = ts.getIdentifier();
        Set<Pair<Integer, TripleTimestamp>> v = updates.get(siteId);
        if (v == null) {
            v = new HashSet<Pair<Integer, TripleTimestamp>>();
            updates.put(siteId, v);
        }
        v.add(new Pair<Integer, TripleTimestamp>(n, ts));
        currentValue += n;
    }

    private int getAggregateOfUpdates() {
        int changes = 0;
        for (Set<Pair<Integer, TripleTimestamp>> v : updates.values()) {
            for (Pair<Integer, TripleTimestamp> vi : v) {
                changes += vi.getFirst();
            }
        }
        return changes;
    }

    private int getAggregateOfPrune() {
        int changes = 0;
        for (Pair<Integer, TripleTimestamp> v : pruneVector.values()) {
            changes += v.getFirst();
        }
        return changes;
    }

    @Override
    protected void mergePayload(IntegerVersioned other) {
        Map<String, Pair<Integer, TripleTimestamp>> newPruneVector = new HashMap<String, Pair<Integer, TripleTimestamp>>();
        for (Entry<String, Pair<Integer, TripleTimestamp>> e : pruneVector.entrySet()) {
            Pair<Integer, TripleTimestamp> v = other.pruneVector.get(e.getKey());
            if (v == null) {
                newPruneVector.put(e.getKey(), e.getValue());
            } else {
                if (e.getValue().getSecond().compareTo(v.getSecond()) < 0) {
                    newPruneVector.put(e.getKey(), v);
                } else {
                    newPruneVector.put(e.getKey(), e.getValue());
                }
                other.pruneVector.remove(e.getKey());
            }
        }
        newPruneVector.putAll(other.pruneVector);
        pruneVector = newPruneVector;
        pruneValue = getAggregateOfPrune();

        cleanUpdatesFromPruned(updates);
        cleanUpdatesFromPruned(other.updates);

        for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> e : other.updates.entrySet()) {
            Set<Pair<Integer, TripleTimestamp>> v = updates.get(e.getKey());
            if (v == null) {
                v = e.getValue();
                updates.put(e.getKey(), new HashSet<Pair<Integer, TripleTimestamp>>(e.getValue()));
            } else {
                v.addAll(e.getValue());
            }
        }

        currentValue = pruneValue + getAggregateOfUpdates();
    }

    private void cleanUpdatesFromPruned(Map<String, Set<Pair<Integer, TripleTimestamp>>> up) {

        Iterator<Entry<String, Set<Pair<Integer, TripleTimestamp>>>> itSites = up.entrySet().iterator();
        while (itSites.hasNext()) {
            Entry<String, Set<Pair<Integer, TripleTimestamp>>> updatesPerSite = itSites.next();
            Iterator<Pair<Integer, TripleTimestamp>> addTSit = updatesPerSite.getValue().iterator();
            while (addTSit.hasNext()) {
                Pair<Integer, TripleTimestamp> ts = addTSit.next();
                if (this.getPruneClock().includes(ts.getSecond())) {
                    addTSit.remove();
                }
            }
            if (updatesPerSite.getValue().isEmpty()) {
                itSites.remove();
            }
        }

    }

    @Override
    public boolean equals(Object other) {
        // TODO do we need to compare objects? should it be oblivious to
        // pruning?
        if (!(other instanceof IntegerVersioned)) {
            return false;
        }
        IntegerVersioned that = (IntegerVersioned) other;
        return that.currentValue == this.currentValue && that.updates.equals(this.updates);
    }

    private int rollbackUpdates(Timestamp rollbackEvent) {
        int delta = 0;
        Iterator<Entry<String, Set<Pair<Integer, TripleTimestamp>>>> itSites = updates.entrySet().iterator();
        while (itSites.hasNext()) {
            Entry<String, Set<Pair<Integer, TripleTimestamp>>> updatesPerSite = itSites.next();
            Iterator<Pair<Integer, TripleTimestamp>> addTSit = updatesPerSite.getValue().iterator();
            while (addTSit.hasNext()) {
                Pair<Integer, TripleTimestamp> ts = addTSit.next();
                if (rollbackEvent.includes(ts.getSecond())) {
                    addTSit.remove();
                    delta += ts.getFirst();
                }
            }
            if (updatesPerSite.getValue().isEmpty()) {
                itSites.remove();
            }
        }
        return delta;
    }

    @Override
    public void rollback(Timestamp rollbackEvent) {
        this.currentValue += rollbackUpdates(rollbackEvent);
    }

    @Override
    protected void pruneImpl(CausalityClock c) {
        int sumOfDeltas = 0;
        Iterator<Entry<String, Set<Pair<Integer, TripleTimestamp>>>> itSites = updates.entrySet().iterator();
        while (itSites.hasNext()) {
            int delta = 0;
            TripleTimestamp pruneMax = null;

            Entry<String, Set<Pair<Integer, TripleTimestamp>>> updatesPerSite = itSites.next();
            Iterator<Pair<Integer, TripleTimestamp>> addTSit = updatesPerSite.getValue().iterator();
            while (addTSit.hasNext()) {
                Pair<Integer, TripleTimestamp> ts = addTSit.next();
                if (c.includes(ts.getSecond())) {
                    addTSit.remove();
                    delta += ts.getFirst();
                    if (pruneMax == null || pruneMax.compareTo(ts.getSecond()) < 0) {
                        pruneMax = ts.getSecond();
                    }
                }
            }

            if (updatesPerSite.getValue().isEmpty()) {
                itSites.remove();
            }

            String siteId = updatesPerSite.getKey();
            Pair<Integer, TripleTimestamp> priorPruned = pruneVector.get(siteId);
            if (priorPruned == null || priorPruned.getFirst() == null) {
                pruneVector.put(siteId, new Pair<Integer, TripleTimestamp>(delta, pruneMax));
            } else {
                Integer priorPruneValue = priorPruned.getFirst();
                pruneVector.put(siteId, new Pair<Integer, TripleTimestamp>(priorPruneValue + delta, pruneMax));
            }
            sumOfDeltas += delta;
        }
        pruneValue += sumOfDeltas;
    }

    protected TxnLocalCRDT<IntegerVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final IntegerVersioned creationState = isRegisteredInStore() ? null : new IntegerVersioned();
        IntegerTxnLocal localView = new IntegerTxnLocal(id, txn, creationState, value(versionClock));
        return localView;
    }

    @Override
    public IntegerVersioned clone() {
        throw new NotSupportedOperationException("FIXME");
    }
}
