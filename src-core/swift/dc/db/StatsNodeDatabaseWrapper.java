package swift.dc.db;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.dc.CRDTData;
import swift.proto.MetadataStatsCollector;
import swift.utils.SafeLog.ReportType;
import sys.scheduler.PeriodicTask;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * DCNodeDatabase wrapper that records statistics on the database.
 * 
 * @author mzawirski
 */
public class StatsNodeDatabaseWrapper implements DCNodeDatabase {
    public static final String DB_STATS_REPORT_PERIOD_SEC_PROP_NAME = "swift.dbstats.reportPeriodSec";
    public static final String DEFAULT_STATS_REPORT_PERIOD_SEC = "1.0";
    private final MetadataStatsCollector statsCollector;
    private final ConcurrentHashMap<String, TableStats> tablesStats = new ConcurrentHashMap<>();
    private final AtomicInteger totalSize = new AtomicInteger();
    private final DCNodeDatabase wrappedDb;

    public StatsNodeDatabaseWrapper(final DCNodeDatabase wrappedDb, final String dcId) {
        this.wrappedDb = wrappedDb;
        this.statsCollector = new MetadataStatsCollector(dcId);
    }

    @Override
    public void init(Properties props) {
        wrappedDb.init(props);
        final double periodSec = Double.parseDouble(props.getProperty(DB_STATS_REPORT_PERIOD_SEC_PROP_NAME,
                DEFAULT_STATS_REPORT_PERIOD_SEC));
        if (statsCollector.isDatabaseTableReportEnabled()) {
            new PeriodicTask(0.0, periodSec) {
                // TODO: DCNodeDatabase does not offer any clean way to stop().
                public void run() {
                    statsCollector.recordDatabaseTableStats("ALL", totalSize.get());
                    for (final Entry<String, TableStats> entry : tablesStats.entrySet()) {
                        statsCollector.recordDatabaseTableStats(entry.getKey(), entry.getValue().getSize());
                    }
                };
            };
        }
    }

    // ASSUMPTION: no concurrent write(i1, ...) write(i2, ...) for i1 == i2
    // Violation may result in incorrect results. Seems to be enforced by
    // DCDataServer.
    @Override
    public boolean write(CRDTIdentifier id, CRDTData<?> data) {
        final boolean wrappedValue = wrappedDb.write(id, data);

        if (ReportType.DATABASE_TABLE_SIZE.isEnabled()) {
            // Optimistic concurrency control optimized for get().
            TableStats tableStats = tablesStats.get(id.getTable());
            if (tableStats == null) {
                final TableStats existingStats = tablesStats.putIfAbsent(id.getTable(), tableStats = new TableStats());
                if (existingStats != null) {
                    tableStats = existingStats;
                }
            }
            tableStats.updateObject(data.getCrdt());
        }
        return wrappedValue;
    }

    private class TableStats {
        private final AtomicInteger tableSize = new AtomicInteger();
        private final ConcurrentHashMap<String, Integer> keySizes = new ConcurrentHashMap<>();

        public int getSize() {
            return tableSize.get();
        }

        public void updateObject(final ManagedCRDT crdt) {
            final int newSize = computeSize(crdt);
            Integer oldSize = keySizes.put(crdt.getUID().getKey(), newSize);
            if (oldSize == null) {
                oldSize = 0;
            }
            final int deltaSize = newSize - oldSize;
            tableSize.addAndGet(deltaSize);
            totalSize.addAndGet(deltaSize);
        }
    }

    @Override
    public void sync(boolean flag) {
        wrappedDb.sync(flag);
    }

    @Override
    public boolean ramOnly() {
        return wrappedDb.ramOnly();
    }

    @Override
    public CRDTData<?> read(CRDTIdentifier id) {
        return wrappedDb.read(id);
    }

    @Override
    public Object readSysData(String table, String key) {
        return wrappedDb.readSysData(table, key);
    }

    @Override
    public boolean writeSysData(String table, String key, Object data) {
        return wrappedDb.writeSysData(table, key, data);
    }

    private int computeSize(ManagedCRDT crdt) {
        final Kryo kryo = statsCollector.getFreshKryo();
        final Output buffer = statsCollector.getFreshKryoBuffer();
        crdt.write(kryo, buffer);
        return buffer.position();
    }
}
