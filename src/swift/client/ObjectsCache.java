package swift.client;

import java.util.*;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;

/**
 * Local cache of CRDT objects.
 * <p>
 * TODO: LRU and other features
 * 
 * @author mzawirski
 */
class ObjectsCache {
    private Map<CRDTIdentifier, Entry> entries;
    
    ObjectsCache() {
        entries = new HashMap<CRDTIdentifier,Entry>();
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

    private static class Entry {
        private final CRDT<?> object;

        private Entry(final CRDT<?> object) {
            this.object = object;
        }

        CRDT<?> getObject() {
            return object;
        }
    }
}
