package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.DirectoryPutUpdate;
import swift.crdt.operations.DirectoryRemoveUpdate;

public class DirectoryTxnLocal extends BaseCRDTTxnLocal<DirectoryVersioned> {
    private Map<String, Map<TripleTimestamp, CRDT<?>>> dir;

    public DirectoryTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, DirectoryVersioned creationState,
            Map<String, Map<TripleTimestamp, CRDT<?>>> payload) {
        super(id, txn, clock, creationState);
        this.dir = payload;
    }

    @Override
    public Object getValue() {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public Object executeQuery(CRDTQuery<DirectoryVersioned> query) {
        return query.executeAt(this);
    }

    public <V extends CRDT<V>> void putNoReturn(String key, V val) {
        // TODO Implement version that returns old value

        // implemented as remove followed by add
        Set<TripleTimestamp> toBeRemoved = new HashSet<TripleTimestamp>();
        Map<TripleTimestamp, CRDT<?>> old = dir.remove(key);
        if (old != null) {
            toBeRemoved.addAll(old.keySet());
        }

        TripleTimestamp ts = nextTimestamp();
        Map<TripleTimestamp, CRDT<?>> entry = new HashMap<TripleTimestamp, CRDT<?>>();
        entry.put(ts, val);
        dir.put(key, entry);

        registerLocalOperation(new DirectoryPutUpdate(key, val, toBeRemoved, ts));
    }

    public void removeNoReturn(String key) {
        // TODO Return removed element?
        Map<TripleTimestamp, CRDT<?>> deleted = dir.remove(key);
        if (deleted != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new DirectoryRemoveUpdate(key, deleted.keySet(), ts));
        }
    }

    public <V> V get(String key) {
        throw new RuntimeException("Not implemented yet!");
    }

    public boolean contains(String key) {
        return dir.containsKey(key);
    }

    // TODO Typecheck dynamically for conformance of key and CRDT type
    // val.getClass().getName() -> gives fully qualified name (FQN)
    // or V.getName()
    //
    // For testing conformance:
    // w = Class.forName(FQN)
    // assert(w == v)
    // v.cast(object)
}
