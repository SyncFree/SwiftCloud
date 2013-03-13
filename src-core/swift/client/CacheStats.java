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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import swift.crdt.CRDTIdentifier;

/**
 * Statistics of Swift scout cache performance.
 * 
 * @author mzawirski
 */
// TODO introduce time dimension
public class CacheStats {
    private Map<String, TableCacheStats> tablesStats;

    public CacheStats() {
        tablesStats = new ConcurrentHashMap<String, TableCacheStats>();
    }

    public void addCacheHit(final CRDTIdentifier id) {
        getOrCreateTableStats(id).cacheHits.incrementAndGet();
    }

    public void addCacheMissNoObject(final CRDTIdentifier id) {
        getOrCreateTableStats(id).cacheMissesNoObject.incrementAndGet();
    }

    public void addCacheMissWrongVersion(final CRDTIdentifier id) {
        getOrCreateTableStats(id).cacheMissesWrongVersion.incrementAndGet();
    }

    public void addCacheMissBizarre(final CRDTIdentifier id) {
        getOrCreateTableStats(id).cacheMissesBizarre.incrementAndGet();
    }

    private TableCacheStats getOrCreateTableStats(final CRDTIdentifier id) {
        TableCacheStats stats;
        // Lock-free read attempt.
        stats = tablesStats.get(id.getTable());
        if (stats == null) {
            // Lock-based creation.
            synchronized (tablesStats) {
                stats = tablesStats.get(id.getTable());
                if (stats == null) {
                    stats = new TableCacheStats();
                    tablesStats.put(id.getTable(), stats);
                }
            }
        }
        return stats;
    }

    /**
     * Print statistics on stdout and resets the counters.
     */
    // TODO export it as an object?
    public void printAndReset() {
        for (final Entry<String, TableCacheStats> statsEntry : tablesStats.entrySet()) {
            final String tableName = statsEntry.getKey();
            final TableCacheStats stats = statsEntry.getValue();
            long _cacheHits = stats.cacheHits.get();
            long _cacheMissesNoObject = stats.cacheMissesNoObject.get();
            long _cacheMissesWrongVersion = stats.cacheMissesWrongVersion.get();
            long _cacheMissesBizarre = stats.cacheMissesBizarre.get();
            long _total = 1 + _cacheHits + _cacheMissesNoObject + _cacheMissesWrongVersion + _cacheMissesBizarre;
            System.err
                    .printf("TABLE=%s, CACHE_RATIO=%.0f, CACHE_HITS=%d,CACHE_MISSES_NO_OBJECT=%d,CACHE_MISSES_WRONG_VERSION=%d,CACHE_MISSES_BIZARRE=%d\n",
                            tableName, 100.0 * _cacheHits / _total, _cacheHits, _cacheMissesNoObject,
                            _cacheMissesWrongVersion, _cacheMissesBizarre);
            stats.reset();
        }
    }

    static class TableCacheStats {
        AtomicLong cacheHits = new AtomicLong();
        AtomicLong cacheMissesNoObject = new AtomicLong();
        AtomicLong cacheMissesWrongVersion = new AtomicLong();
        AtomicLong cacheMissesBizarre = new AtomicLong();

        private void reset() {
            cacheHits.set(0);
            cacheMissesNoObject.set(0);
            cacheMissesWrongVersion.set(0);
            cacheMissesBizarre.set(0);
        }
    }
}
