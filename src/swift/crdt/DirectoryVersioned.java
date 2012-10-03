package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.payload.PayloadHelper;

@SuppressWarnings("serial")
public class DirectoryVersioned extends BaseCRDT<DirectoryVersioned> {
    private Map<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>> dir;

    public DirectoryVersioned() {
        this.dir = new HashMap<CRDTIdentifier, Map<TripleTimestamp, Set<TripleTimestamp>>>();
    }

    @Override
    public void rollback(Timestamp rollbackEvent) {
        PayloadHelper.rollback(this.dir, rollbackEvent);
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        final List<TripleTimestamp> releasedTimestampUsages = PayloadHelper.pruneImpl(this.dir, pruningPoint);
        for (final TripleTimestamp ts : releasedTimestampUsages) {
            unregisterTimestampUsage(ts);
        }
    }

    @Override
    protected void mergePayload(DirectoryVersioned other) {
        final List<TripleTimestamp> newTimestampUsages = new LinkedList<TripleTimestamp>();
        final List<TripleTimestamp> releasedTimestampUsages = new LinkedList<TripleTimestamp>();
        PayloadHelper.mergePayload(this.dir, this.getClock(), other.dir, other.getClock(), newTimestampUsages,
                releasedTimestampUsages);
        for (final TripleTimestamp ts : newTimestampUsages) {
            registerTimestampUsage(ts);
        }
        for (final TripleTimestamp ts : releasedTimestampUsages) {
            unregisterTimestampUsage(ts);
        }
    }

    protected void execute(CRDTUpdate<DirectoryVersioned> op) {
        op.applyTo(this);
    }

    public void applyPut(CRDTIdentifier entry, TripleTimestamp uid) {
        Map<TripleTimestamp, Set<TripleTimestamp>> meta = dir.get(entry);
        if (meta == null) {
            meta = new HashMap<TripleTimestamp, Set<TripleTimestamp>>();
            dir.put(entry, meta);
        }
        meta.put(uid, new HashSet<TripleTimestamp>());
        registerTimestampUsage(uid);
    }

    public void applyRemove(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp uid) {
        Map<TripleTimestamp, Set<TripleTimestamp>> s = dir.get(key);
        if (s == null) {
            return;
        }

        for (TripleTimestamp ts : toBeRemoved) {
            Set<TripleTimestamp> removals = s.get(ts);
            if (removals != null) {
                removals.add(uid);
                registerTimestampUsage(uid);
            }
        }
    }

    @Override
    protected TxnLocalCRDT<DirectoryVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        Map<CRDTIdentifier, Set<TripleTimestamp>> payload = PayloadHelper.getValue(this.dir, versionClock);
        final DirectoryVersioned creationState = isRegisteredInStore() ? null : new DirectoryVersioned();
        DirectoryTxnLocal localView = new DirectoryTxnLocal(id, txn, versionClock, creationState, payload);
        return localView;
    }

}
