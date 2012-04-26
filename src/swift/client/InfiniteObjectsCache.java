package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.operations.CRDTObjectOperationsGroup;

/**
 * Local cache of CRDT objects.
 * <p>
 * Extract interface, add LRU and other features.
 * 
 * @author mzawirski
 */
class InfiniteObjectsCache {
    private Map<CRDTIdentifier, Entry> entries;

    InfiniteObjectsCache() {
        entries = new HashMap<CRDTIdentifier, Entry>();
    }

    void add(final CRDT<?> object) {
        final Entry entry = new Entry(object);
        entries.put(object.getUID(), entry);
    }

    CRDT<?> get(final CRDTIdentifier id) {
        final Entry entry = entries.get(id);
        if (entry == null) {
            return null;
        }
        return entry.getObject();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    void recordOnAll(final Timestamp timestamp) {
        final CRDTObjectOperationsGroup dummyOp = new CRDTObjectOperationsGroup(null, timestamp, null);
        for (final Entry entry : entries.values()) {
            entry.object.execute(dummyOp, CRDTOperationDependencyPolicy.IGNORE);
        }
    }

    private static final class Entry {
        private final CRDT<?> object;

        private Entry(final CRDT<?> object) {
            this.object = object;
        }

        CRDT<?> getObject() {
            return object;
        }
    }
}
