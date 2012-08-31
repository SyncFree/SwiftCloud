package swift.crdt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.DirectoryPutUpdate;
import swift.crdt.operations.DirectoryRemoveUpdate;
import swift.utils.Pair;

public class DirectoryTxnLocal extends BaseCRDTTxnLocal<DirectoryVersioned> {
    private Map<String, Pair<CRDT<?>, Set<Timestamp>>> dir;

    public DirectoryTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, DirectoryVersioned creationState) {
        super(id, txn, clock, creationState);
        throw new RuntimeException("Not implemented yet!");
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
        Set<Timestamp> toBeRemoved = new HashSet<Timestamp>();
        Pair<CRDT<?>, Set<Timestamp>> old = dir.remove(key);
        if (old != null) {
            toBeRemoved.addAll(old.getSecond());
        }

        Timestamp ts = nextTimestamp();
        Set<Timestamp> tset = new HashSet<Timestamp>();
        tset.add(ts);
        dir.put(key, new Pair<CRDT<?>, Set<Timestamp>>(val, tset));

        registerLocalOperation(new DirectoryPutUpdate(key, val, toBeRemoved, ts));
    }

    public void removeNoReturn(String key) {
        // TODO Return removed element?
        Pair<CRDT<?>, Set<Timestamp>> deleted = dir.remove(key);
        if (deleted != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new DirectoryRemoveUpdate(key, deleted.getSecond()));
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
