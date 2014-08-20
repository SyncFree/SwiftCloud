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
package swift.dc.db;

import java.util.Properties;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.ManagedCRDT;
import swift.dc.CRDTData;
import swift.proto.MetadataStatsCollector;
import swift.utils.DatabaseSizeStats;
import swift.utils.SafeLog.ReportType;

/**
 * DCNodeDatabase wrapper that records statistics on the database. Enabled with
 * {@link ReportType#DATABASE_TABLE_SIZE}.
 * <p>
 * Thread unsafe (assumes external synchronization) for concurrent updates of
 * the same object.
 * 
 * @author mzawirski
 */
public class StatsNodeDatabaseWrapper implements DCNodeDatabase {
    private final DCNodeDatabase wrappedDb;
    private final DatabaseSizeStats stats;

    public StatsNodeDatabaseWrapper(final DCNodeDatabase wrappedDb, final String dcId) {
        this.wrappedDb = wrappedDb;
        this.stats = new DatabaseSizeStats(new MetadataStatsCollector(dcId));
    }

    @Override
    public void init(Properties props) {
        stats.init(props);
        wrappedDb.init(props);
    }

    // ASSUMPTION: no concurrent write(i1, ...) write(i2, ...) for i1 == i2
    // Violation may result in incorrect results. Seems to be enforced by
    // DCDataServer.
    @Override
    public boolean write(CRDTIdentifier id, CRDTData<?> data) {
        final boolean wrappedValue = wrappedDb.write(id, data);
        final ManagedCRDT<?> crdt = data.getCrdt();
        stats.updateObject(id, crdt);
        return wrappedValue;
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
}
