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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
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
 * @author smduarte, mzawirski
 */
class LRUObjectsCache {

    public static interface EvictionListener {
        void onEviction(CRDTIdentifier id);
    }

    private static Logger logger = Logger.getLogger(LRUObjectsCache.class.getName());

    private final int maxElements;
    private final long evictionTimeMillis;
    private Map<CRDTIdentifier, Entry> entries;
    private Map<CRDTIdentifier, Entry> shadowEntries;
    private Set<Long> evictionProtections;

    private EvictionListener evictionListener = new EvictionListener() {
        public void onEviction(CRDTIdentifier id) {
        }
    };

    List<Long> foo = new ArrayList<Long>();

    synchronized public void removeProtection(long serial) {
        evictionProtections.remove(serial);
        evictExcess();
        evictOutdated();
    }

    /**
     * @param evictionTimeMillis
     *            maximum life-time for object entries (exclusive) in
     *            milliseconds
     */
    @SuppressWarnings("serial")
    public LRUObjectsCache(final long evictionTimeMillis, final int maxElements) {

        this.evictionTimeMillis = evictionTimeMillis;
        this.maxElements = maxElements;

        entries = new LinkedHashMap<CRDTIdentifier, Entry>(32, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<CRDTIdentifier, Entry> eldest) {
                Entry e = eldest.getValue();
                if (!evictionProtections.contains(e.id()) && size() > maxElements) {
                    shadowEntries.remove(eldest.getKey());
                    evictionListener.onEviction(eldest.getKey());

                    // System.err.println(eldest.getKey() +
                    // " evicted from the cache due to size limit, acesses:"
                    // + e.getNumberOfAccesses());

                    logger.info("Object evicted from the cache due to size limit, acesses:" + e.getNumberOfAccesses());
                    return true;
                } else
                    return false;
            }
        };
        shadowEntries = new HashMap<CRDTIdentifier, Entry>();
        evictionProtections = new HashSet<Long>();
    }

    void setEvictionListener(EvictionListener evictionListener) {
        this.evictionListener = evictionListener;
    }

    /**
     * Adds object to the cache, possibly overwriting old entry. May cause
     * evictoin due to size limit in the cache.
     * 
     * @param object
     *            object to add
     */
    synchronized public void add(final CRDT<?> object, long txnSerial) {
        evictionProtections.add(txnSerial);
        Entry e = new Entry(object, txnSerial);
        entries.put(object.getUID(), e);
        shadowEntries.put(object.getUID(), e);
    }

    /**
     * Returns object for given id and records access to the cache.
     * 
     * @param id
     *            object id
     * @return object or null if object is absent in the cache
     */
    synchronized public CRDT<?> getAndTouch(final CRDTIdentifier id) {
        final Entry entry = entries.get(id);
        if (entry == null) {
            return null;
        }
        entry.touch();
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
    synchronized public CRDT<?> getWithoutTouch(final CRDTIdentifier id) {
        final Entry entry = entries.get(id);
        return entry == null ? null : entry.getObject();
    }

    /**
     * Evicts all objects that have not been accessed for over
     * evictionTimeMillis specified for this cache.
     */
    private void evictExcess() {
        int evictedObjects = 0;
        int excess = entries.size() - maxElements;
        for (Iterator<Map.Entry<CRDTIdentifier, Entry>> it = entries.entrySet().iterator(); it.hasNext();) {
            if (evictedObjects < excess) {
                Map.Entry<CRDTIdentifier, Entry> e = it.next();
                final Entry val = e.getValue();
                if (!evictionProtections.contains(val.id())) {
                    it.remove();
                    evictedObjects++;
                    shadowEntries.remove(e.getKey());
                    evictionListener.onEviction(e.getKey());
                    // System.err.println( e.getKey() +
                    // " evicted from the cache due to size limit, acesses:" +
                    // e.getValue().getNumberOfAccesses());
                }
            } else
                break;
        }
        if (evictedObjects > 0) {
            // System.err.printf("Objects evicted from the cache due to excessive size: %s / %s / %s\n",
            // evictedObjects,
            // entries.size(), maxElements);
            logger.info(evictedObjects + " objects evicted from the cache due to timeout");
        }
    }

    /**
     * Evicts all objects that have not been accessed for over
     * evictionTimeMillis specified for this cache.
     */
    private void evictOutdated() {
        long now = System.currentTimeMillis();
        final long evictionThreashold = now - evictionTimeMillis;

        int evictedObjects = 0;
        for (Iterator<Map.Entry<CRDTIdentifier, Entry>> it = entries.entrySet().iterator(); it.hasNext();) {
            Map.Entry<CRDTIdentifier, Entry> e = it.next();
            final Entry entry = e.getValue();
            if (entry.getLastAcccessTimeMillis() <= evictionThreashold) {
                it.remove();
                evictedObjects++;
                shadowEntries.remove(e.getKey());
                evictionListener.onEviction(e.getKey());
            } else {
                break;
            }
        }
        // if (evictedObjects > 0)
        // System.err.println(evictedObjects +
        // " objects evicted from the cache due to timeout");

        logger.info(evictedObjects + " objects evicted from the cache due to timeout");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    synchronized void recordOnAll(final TimestampMapping timestampMapping) {
        final CRDTObjectUpdatesGroup dummyOp = new CRDTObjectUpdatesGroup(null, timestampMapping, null, null);
        for (final Entry entry : entries.values()) {
            entry.object.execute(dummyOp, CRDTOperationDependencyPolicy.IGNORE);
        }
    }

    synchronized void augmentWithCausalClock(final CausalityClock causalClock) {
        for (final Entry entry : entries.values()) {
            entry.object.augmentWithDCClock(causalClock);
        }
    }

    synchronized void printStats() {
        SortedSet<Entry> se = new TreeSet<Entry>(entries.values());
        for (Entry i : se)
            System.err.println(i.object.getUID() + "/" + i.accesses);
    }

    static AtomicLong g_serial = new AtomicLong();

    private final class Entry implements Comparable<Entry> {
        private final CRDT<?> object;
        private long lastAccessTimeMillis;
        private long accesses;
        private long txnId;
        private long serial = g_serial.incrementAndGet();

        public Entry(final CRDT<?> object, long txnId) {
            this.object = object;
            this.txnId = txnId;
            touch();
        }

        public long id() {
            return txnId;
        }

        public CRDT<?> getObject() {
            return object;
        }

        public long getLastAcccessTimeMillis() {
            return lastAccessTimeMillis;
        }

        public long getNumberOfAccesses() {
            return accesses;
        }

        public void touch() {
            accesses++;
            lastAccessTimeMillis = System.currentTimeMillis();
        }

        @Override
        public int compareTo(Entry other) {
            if (accesses == other.accesses)
                return serial < other.serial ? -1 : 1;
            else
                return accesses < other.accesses ? -1 : 1;
        }
    }
}
