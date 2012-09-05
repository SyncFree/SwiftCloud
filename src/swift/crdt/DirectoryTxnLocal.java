package swift.crdt;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.DirectoryPutUpdate;
import swift.crdt.operations.DirectoryRemoveUpdate;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public class DirectoryTxnLocal extends BaseCRDTTxnLocal<DirectoryVersioned> {
    private Map<String, Map<TripleTimestamp, CRDTIdentifier>> dir;

    public DirectoryTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, DirectoryVersioned creationState,
            Map<String, Map<TripleTimestamp, CRDTIdentifier>> payload) {
        super(id, txn, clock, creationState);
        this.dir = payload;
    }

    @Override
    public Map<String, Collection<CRDTIdentifier>> getValue() {
        Map<String, Collection<CRDTIdentifier>> val = new HashMap<String, Collection<CRDTIdentifier>>();
        for (Entry<String, Map<TripleTimestamp, CRDTIdentifier>> e : dir.entrySet()) {
            String cname = getClassName(e.getKey());
            val.put(cname, e.getValue().values());
        }
        return val;
    }

    public String getClassName(String key) {
        return key.split(":")[1];
    }

    public String getEntryName(String key) {
        return key.split(":")[0];
    }

    public <V extends CRDT<V>> String getDirEntry(String name, Class<V> c) {
        return name + ":" + c.getName();
    }

    @Override
    public Object executeQuery(CRDTQuery<DirectoryVersioned> query) {
        return query.executeAt(this);
    }

    public <V extends CRDT<V>> void putNoReturn(String key, CRDTIdentifier val, Class<V> c) {
        // TODO Implement version that returns old value

        // implemented as remove followed by add
        Set<TripleTimestamp> toBeRemoved = new HashSet<TripleTimestamp>();
        Map<TripleTimestamp, CRDTIdentifier> old = dir.remove(getDirEntry(key, c));
        if (old != null) {
            toBeRemoved.addAll(old.keySet());
        }

        TripleTimestamp ts = nextTimestamp();
        Map<TripleTimestamp, CRDTIdentifier> entry = new HashMap<TripleTimestamp, CRDTIdentifier>();
        entry.put(ts, val);
        dir.put(getDirEntry(key, c), entry);

        registerLocalOperation(new DirectoryPutUpdate(getDirEntry(key, c), val, toBeRemoved, ts));
    }

    public void removeNoReturn(String key) {
        // TODO Return removed element?
        Map<TripleTimestamp, CRDTIdentifier> deleted = dir.remove(key);
        if (deleted != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new DirectoryRemoveUpdate(key, deleted.keySet(), ts));
        }
    }

    public <L, V extends CRDT<V>> L get(String key, Class<V> c) {
        Map<TripleTimestamp, CRDTIdentifier> entries = dir.get(getDirEntry(key, c));
        for (CRDTIdentifier e : entries.values()) {
            try {
                TxnLocalCRDT<?> obj = this.getTxnHandle().get(e, false, c);
                return (L) obj;
            } catch (WrongTypeException e1) {
            } catch (NoSuchObjectException e1) {
            } catch (VersionNotFoundException e1) {
            } catch (NetworkException e1) {
            }
            // TODO Does it make sense to lift any of these exceptions to the
            // application code?
        }
        return null;
    }

    public <V extends CRDT<V>> boolean contains(String key, Class<V> c) {
        return dir.containsKey(getDirEntry(key, c));
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
