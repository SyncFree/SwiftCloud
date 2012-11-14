/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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
 * Local cache of CRDT objects with LRU eviction policy. Elements get evicted
 * when not used for a defined period of time or size of the cache is exceeded.
 * <p>
 * Thread unsafe (requires external synchronization).
 * 
 * @author mzawirski
 */
class TimeSizeBoundedObjectsCache {
    private static Logger logger = Logger.getLogger(TimeSizeBoundedObjectsCache.class.getName());

    private final long evictionTimeMillis;
    private final int maxElements;
    private Map<CRDTIdentifier, Entry> entries;
    private LinkedHashSet<Entry> entriesEvictionQueue;

    /**
     * @param evictionTimeMillis
     *            maximum life-time for object entries (exclusive) in
     *            milliseconds
     */
    public TimeSizeBoundedObjectsCache(final long evictionTimeMillis, final int maxElements) {
        this.evictionTimeMillis = evictionTimeMillis;
        this.maxElements = maxElements;
        entries = new HashMap<CRDTIdentifier, Entry>();
        entriesEvictionQueue = new LinkedHashSet<TimeSizeBoundedObjectsCache.Entry>();
    }

    /**
     * Adds object to the cache, possibly overwriting old entry. May cause
     * evictoin due to size limit in the cache.
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
        evictOversized();
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
        logger.info(evictedObjects + " objects evicted from the cache due to timeout");
    }

    private void evictOversized() {
        final Iterator<Entry> iter = entriesEvictionQueue.iterator();
        final int entriesToRemove = entries.size() - maxElements;
        if (entriesToRemove <= 0) {
            return;
        }
        for (int i = 0; i < entriesToRemove; i++) {
            final Entry entry = iter.next();
            entries.remove(entry.object.getUID());
            iter.remove();
        }
        logger.info(entriesToRemove + " objects evicted from the cache due to size limit");
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
