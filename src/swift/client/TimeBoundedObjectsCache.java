package swift.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.logging.Logger;

import swift.clocks.TimestampMapping;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.operations.CRDTObjectUpdatesGroup;

/**
 * Local cache of CRDT objects with LRU eviction policy.
 * <p>
 * Thread unsafe (requires external synchronization).
 * 
 * @author mzawirski
 */
class TimeBoundedObjectsCache {
    private static Logger logger = Logger.getLogger(TimeBoundedObjectsCache.class.getName());

    private final long evictionTimeMillis;
    private Map<CRDTIdentifier, Entry> entries;
    private LinkedHashSet<Entry> entriesEvictionQueue;

    /**
     * @param evictionTimeMillis
     *            maximum life-time for object entries (exclusive) in
     *            milliseconds
     */
    public TimeBoundedObjectsCache(final long evictionTimeMillis) {
        this.evictionTimeMillis = evictionTimeMillis;
        entries = new HashMap<CRDTIdentifier, Entry>();
        entriesEvictionQueue = new LinkedHashSet<TimeBoundedObjectsCache.Entry>();
    }

    /**
     * Adds object to the cache, possibly overwriting old entry.
     * 
     * @param object
     *            object to add
     */
    public void add(final CRDT<?> object) {
        final Entry entry = new Entry(object);
        final Entry oldEntry = entries.put(object.getUID(), entry);
        if (oldEntry != null) {
            entriesEvictionQueue.remove(oldEntry);
        }
        entriesEvictionQueue.add(entry);
    }

    /**
     * Returns object for given id and records access to the cache.
     * 
     * @param id
     *            object id
     * @return object or null if object is absent in the cache
     */
    public CRDT<?> getAndTouch(final CRDTIdentifier id) {
        final Entry entry = entries.get(id);
        if (entry == null) {
            return null;
        }
        entry.touch();
        entriesEvictionQueue.remove(entry);
        entriesEvictionQueue.add(entry);
        return entry.getObject();
    }

    /**
     * Returns object for given id without recording access to the cache (in
     * terms of eviction policy).
     * 
     * @param id
     *            object id
     * @return object or null if object is absent in the cache
     */
    public CRDT<?> getWithoutTouch(final CRDTIdentifier id) {
        final Entry entry = entries.get(id);
        if (entry == null) {
            return null;
        }
        return entry.getObject();
    }

    /**
     * Evicts all objects that has not been accessed for over evictionTimeMillis
     * specified for this cache.
     */
    public void evictOutdated() {
        final long evictionThreashold = System.currentTimeMillis() - evictionTimeMillis;

        final Iterator<Entry> iter = entriesEvictionQueue.iterator();
        int evictedObjects = 0;
        while (iter.hasNext()) {
            final Entry entry = iter.next();
            if (entry.getLastAcccessTimeMillis() <= evictionThreashold) {
                entries.remove(entry);
                iter.remove();
                evictedObjects++;
            } else {
                break;
            }
        }
        logger.info(evictedObjects + " objects evicted from the cache");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    void recordOnAll(final TimestampMapping timestampMapping) {
        final CRDTObjectUpdatesGroup dummyOp = new CRDTObjectUpdatesGroup(null, timestampMapping, null, null);
        for (final Entry entry : entries.values()) {
            entry.object.execute(dummyOp, CRDTOperationDependencyPolicy.IGNORE);
        }
    }

    private static final class Entry {
        private final CRDT<?> object;
        private long lastAccessTimeMillis;

        public Entry(final CRDT<?> object) {
            this.object = object;
            touch();
        }

        public CRDT<?> getObject() {
            return object;
        }

        public long getLastAcccessTimeMillis() {
            return lastAccessTimeMillis;
        }

        public void touch() {
            lastAccessTimeMillis = System.currentTimeMillis();
        }
    }
}
