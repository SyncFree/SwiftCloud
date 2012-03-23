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
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.IntegerAdd;
import swift.crdt.operations.IntegerSub;
import swift.exceptions.NotSupportedOperationException;
import swift.utils.Pair;

public class IntegerVersioned extends BaseCRDT<IntegerVersioned> {
    private static final long serialVersionUID = 1L;
    private Map<String, Set<Pair<Integer, TripleTimestamp>>> adds;
    private Map<String, Set<Pair<Integer, TripleTimestamp>>> rems;
    // Current value with respect to the updatesClock
    private int currentValue;

    // Value with respect to the pruneClock
    private Map<String, Integer> pruneVector;
    private int pruneValue;

    public IntegerVersioned() {
        this.adds = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
        this.rems = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
    }

    private int value(CausalityClock snapshotClock) {
        if (snapshotClock.compareTo(getClock()) != CMP_CLOCK.CMP_ISDOMINATED) {
            // Since snapshot covers all updates making up this object, use the
            // current value.
            return currentValue;
        }
        int retValue = pruneValue;
        retValue += filterUpdates(snapshotClock, this.adds);
        retValue -= filterUpdates(snapshotClock, this.rems);
        return retValue;
    }

    private int filterUpdates(CausalityClock clk, Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
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

    private void addU(int n, TripleTimestamp ts) {
        if (n < 0) {
            subU(-n, ts);
        }
        applyUpdate(n, ts, this.adds);
    }

    private void subU(int n, TripleTimestamp ts) {
        if (n < 0) {
            addU(-n, ts);
        }
        applyUpdate(n, ts, this.rems);
    }

    private void applyUpdate(int n, TripleTimestamp ts, Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
        String siteId = ts.getIdentifier();
        Set<Pair<Integer, TripleTimestamp>> v = updates.get(siteId);
        if (v == null) {
            v = new HashSet<Pair<Integer, TripleTimestamp>>();
            updates.put(siteId, v);
        }
        v.add(new Pair<Integer, TripleTimestamp>(n, ts));
    }

    private void mergeUpdates(Map<String, Set<Pair<Integer, TripleTimestamp>>> mine,
            Map<String, Set<Pair<Integer, TripleTimestamp>>> other) {
        for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> e : other.entrySet()) {
            Set<Pair<Integer, TripleTimestamp>> v = mine.get(e.getKey());
            if (v == null) {
                v = e.getValue();
                mine.put(e.getKey(), new HashSet<Pair<Integer, TripleTimestamp>>(e.getValue()));
            } else {
                v.addAll(e.getValue());
            }
        }
    }

    private int getAggregateOfUpdates(Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
        int changes = 0;
        for (Set<Pair<Integer, TripleTimestamp>> v : updates.values()) {
            for (Pair<Integer, TripleTimestamp> vi : v) {
                changes += vi.getFirst();
            }
        }
        return changes;
    }

    @Override
    protected void mergePayload(IntegerVersioned other) {
        mergeUpdates(this.adds, other.adds);
        mergeUpdates(this.rems, other.rems);

        this.currentValue = getAggregateOfUpdates(this.adds);
        this.currentValue -= getAggregateOfUpdates(this.rems);
    }

    @Override
    public boolean equals(Object other) {
        // TODO do we need to compare objects? should it be oblivious to
        // pruning?
        if (!(other instanceof IntegerVersioned)) {
            return false;
        }
        IntegerVersioned that = (IntegerVersioned) other;
        return that.currentValue == this.currentValue && that.adds.equals(this.adds) && that.rems.equals(this.rems);
    }

    private int rollbackUpdates(Timestamp rollbackEvent, Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
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
        this.currentValue -= rollbackUpdates(rollbackEvent, this.adds);
        this.currentValue += rollbackUpdates(rollbackEvent, this.rems);
    }

    @Override
    protected void executeImpl(CRDTOperation op) {
        if (op instanceof IntegerAdd) {
            IntegerAdd addop = (IntegerAdd) op;
            this.addU(addop.getVal(), addop.getTimestamp());
        } else if (op instanceof IntegerSub) {
            IntegerSub subop = (IntegerSub) op;
            this.subU(subop.getVal(), subop.getTimestamp());
        } else {
            throw new NotSupportedOperationException("Operation " + op + " is not supported for CRDT " + this.id);
        }
    }

    private int pruneUpdates(CausalityClock clck, Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
        int sumOfDeltas = 0;
        Iterator<Entry<String, Set<Pair<Integer, TripleTimestamp>>>> itSites = updates.entrySet().iterator();
        while (itSites.hasNext()) {
            int delta = 0;
            Entry<String, Set<Pair<Integer, TripleTimestamp>>> updatesPerSite = itSites.next();
            Iterator<Pair<Integer, TripleTimestamp>> addTSit = updatesPerSite.getValue().iterator();
            while (addTSit.hasNext()) {
                Pair<Integer, TripleTimestamp> ts = addTSit.next();
                if (clck.includes(ts.getSecond())) {
                    addTSit.remove();
                    delta += ts.getFirst();
                }
            }
            if (updatesPerSite.getValue().isEmpty()) {
                itSites.remove();
            }
            String siteId = updatesPerSite.getKey();
            Integer priorPruneValue = pruneVector.get(siteId);
            if (priorPruneValue == null) {
                pruneVector.put(siteId, delta);
            } else {
                pruneVector.put(siteId, priorPruneValue + delta);
            }
            sumOfDeltas += delta;
        }
        return sumOfDeltas;
    }

    @Override
    protected void pruneImpl(CausalityClock c) {
        pruneValue += pruneUpdates(c, adds);
        pruneValue -= pruneUpdates(c, rems);
    }

    public TxnLocalCRDT<IntegerVersioned> getTxnLocalCopy(CausalityClock pruneClock, CausalityClock versionClock,
            TxnHandle txn) {

        IntegerTxnLocal localView = new IntegerTxnLocal(id, txn, versionClock, value(versionClock));
        return localView;
    }
}
