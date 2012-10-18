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
            System.out
                    .printf("TABLE=%s,CACHE_HITS=%d,CACHE_MISSES_NO_OBJECT=%d,CACHE_MISSES_WRONG_VERSION=%d,CACHE_MISSES_BIZARRE=%d\n",
                            tableName, stats.cacheHits.get(), stats.cacheMissesNoObject.get(),
                            stats.cacheMissesWrongVersion.get(), stats.cacheMissesBizarre.get());
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
