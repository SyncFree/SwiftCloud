/*****************************************************************************
 * Copyright 2014 INRIA
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
package swift.utils;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.proto.MetadataStatsCollector;
import swift.utils.SafeLog.ReportType;
import sys.scheduler.PeriodicTask;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * Statistics of object database size, aggregated by table names. Enabled with
 * {@link ReportType#DATABASE_TABLE_SIZE}.
 * <p>
 * Thread unsafe (assumes external synchronization) for concurrent updates of
 * the same object.
 * 
 * @author mzawirski
 */
public class DatabaseSizeStats {
    public static final String DB_STATS_REPORT_PERIOD_SEC_PROP_NAME = "swift.dbReportPeriodSec";
    public static final String DEFAULT_STATS_REPORT_PERIOD_SEC = "10.0";
    private final MetadataStatsCollector statsCollector;
    private final ConcurrentHashMap<String, TableStats> tablesStats = new ConcurrentHashMap<>();
    private final AtomicInteger totalSize = new AtomicInteger();

    public DatabaseSizeStats(final MetadataStatsCollector statsCollector) {
        this.statsCollector = statsCollector;
    }

    public void init(Properties props) {
        final double periodSec = Double.parseDouble(props.getProperty(DB_STATS_REPORT_PERIOD_SEC_PROP_NAME,
                DEFAULT_STATS_REPORT_PERIOD_SEC));
        if (statsCollector.isDatabaseTableReportEnabled()) {
            new PeriodicTask(0.0, periodSec) {
                // TODO: implement stop()
                public void run() {
                    statsCollector.recordDatabaseTableStats("ALL", totalSize.get());
                    for (final Entry<String, TableStats> entry : tablesStats.entrySet()) {
                        statsCollector.recordDatabaseTableStats(entry.getKey(), entry.getValue().getSize());
                    }
                };
            };
        }
    }

    /**
     * Informs of object removal.
     * 
     * @param id
     */
    public void removeObject(CRDTIdentifier id) {
        updateObject(id, null);
    }

    /**
     * Informs of object update that could have potentially caused changes in
     * its size.
     * 
     * @param id
     *            object id
     * @param crdt
     *            object
     */
    public void updateObject(CRDTIdentifier id, final ManagedCRDT<?> crdt) {
        if (ReportType.DATABASE_TABLE_SIZE.isEnabled()) {
            // Optimistic concurrency control optimized for get().
            TableStats tableStats = tablesStats.get(id.getTable());
            if (tableStats == null) {
                final TableStats existingStats = tablesStats.putIfAbsent(id.getTable(), tableStats = new TableStats());
                if (existingStats != null) {
                    tableStats = existingStats;
                }
            }
            tableStats.updateObject(id.getKey(), crdt);
        }
    }

    private class TableStats {
        private final AtomicInteger tableSize = new AtomicInteger();
        private final ConcurrentHashMap<String, Integer> keySizes = new ConcurrentHashMap<>();

        public int getSize() {
            return tableSize.get();
        }

        public void updateObject(final String key, final ManagedCRDT crdt) {
            final int newSize;
            Integer oldSize;
            if (crdt == null) {
                newSize = 0;
                oldSize = keySizes.remove(key);
            } else {
                newSize = computeSize(crdt);
                oldSize = keySizes.put(key, newSize);
            }
            if (oldSize == null) {
                oldSize = 0;
            }
            final int deltaSize = newSize - oldSize;
            tableSize.addAndGet(deltaSize);
            totalSize.addAndGet(deltaSize);
        }
    }

    private int computeSize(ManagedCRDT crdt) {
        final Kryo kryo = statsCollector.getFreshKryo();
        final Output buffer = statsCollector.getFreshKryoBuffer();
        crdt.write(kryo, buffer);
        return buffer.position();
    }

}
