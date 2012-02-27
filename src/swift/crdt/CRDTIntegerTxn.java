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
import swift.crdt.interfaces.ICRDTInteger;
import swift.crdt.operations.IntegerAdd;
import swift.crdt.operations.IntegerSub;
import swift.exceptions.NotSupportedOperationException;
import swift.utils.Pair;

public class CRDTIntegerTxn extends BaseCRDT<CRDTIntegerTxn, ICRDTInteger> {
    private Map<String, Set<Pair<Integer, TripleTimestamp>>> adds;
    private Map<String, Set<Pair<Integer, TripleTimestamp>>> rems;
    private int val;

    public CRDTIntegerTxn() {
        this.val = 0;
        this.adds = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
        this.rems = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
    }

    public int value() {
        return this.val;
    }

    public int value(CausalityClock clk) {
        int retValue = 0;
        retValue += filterUpdates(clk, this.adds);
        retValue -= filterUpdates(clk, this.rems);
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

    public void add(int n) {
        TripleTimestamp ts = getTxnHandle().nextTimestamp();
        CRDTOperation op = new IntegerAdd(getUID(), ts, getClock(), n);
        getTxnHandle().registerOperation(op);
    }

    public void sub(int n) {
        TripleTimestamp ts = getTxnHandle().nextTimestamp();
        CRDTOperation op = new IntegerSub(getUID(), ts, getClock(), n);
        getTxnHandle().registerOperation(op);
    }

    public int addU(int n, TripleTimestamp ts) {
        if (n < 0) {
            return subU(-n, ts);
        }
        applyUpdate(n, ts, this.adds);
        val += n;
        return val;
    }

    public int subU(int n, TripleTimestamp ts) {
        if (n < 0) {
            return addU(-n, ts);
        }
        applyUpdate(n, ts, this.rems);
        val -= n;
        return val;
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

    private int allUpdates(Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
        int changes = 0;
        for (Set<Pair<Integer, TripleTimestamp>> v : updates.values()) {
            for (Pair<Integer, TripleTimestamp> vi : v) {
                changes += vi.getFirst();
            }
        }
        return changes;
    }

    @Override
    public void mergePayload(CRDTIntegerTxn other) {
        mergeUpdates(this.adds, other.adds);
        mergeUpdates(this.rems, other.rems);

        this.val = allUpdates(this.adds);
        this.val -= allUpdates(this.rems);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CRDTIntegerTxn)) {
            return false;
        }
        CRDTIntegerTxn that = (CRDTIntegerTxn) other;
        return that.val == this.val && that.adds.equals(this.adds) && that.rems.equals(this.rems);
    }

    private int rollbackUpdates(Timestamp rollbackEvent, Map<String, Set<Pair<Integer, TripleTimestamp>>> updates) {
        int delta = 0;
        Iterator<Entry<String, Set<Pair<Integer, TripleTimestamp>>>> it = updates.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<Pair<Integer, TripleTimestamp>>> addForSite = it.next();
            Iterator<Pair<Integer, TripleTimestamp>> addTSit = addForSite.getValue().iterator();
            while (addTSit.hasNext()) {
                Pair<Integer, TripleTimestamp> ts = addTSit.next();
                if ((ts.getSecond()).equals(rollbackEvent)) {
                    addTSit.remove();
                    delta += ts.getFirst();
                }
            }
            if (addForSite.getValue().isEmpty()) {
                it.remove();
            }
        }
        return delta;
    }

    @Override
    public void rollback(Timestamp rollbackEvent) {
        this.val -= rollbackUpdates(rollbackEvent, this.adds);
        this.val += rollbackUpdates(rollbackEvent, this.rems);
    }

    @Override
    public void execute(ICRDTInteger op) {
        if (op instanceof IntegerAdd) {
            IntegerAdd addop = (IntegerAdd) op;
            this.addU(addop.getVal(), addop.getTimestamp());
        } else if (op instanceof IntegerSub) {
            IntegerSub subop = (IntegerSub) op;
            this.subU(subop.getVal(), subop.getTimestamp());
        } else {
            throw new NotSupportedOperationException();
        }

    }

    @Override
    public void prune(CausalityClock c) {
        // TODO Auto-generated method stub

    }
}
