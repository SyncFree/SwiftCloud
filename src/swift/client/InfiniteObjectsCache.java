package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;

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
