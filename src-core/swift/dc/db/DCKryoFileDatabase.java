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
package swift.dc.db;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;
import sys.net.impl.KryoLib;
import sys.scheduler.Task;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DCKryoFileDatabase implements DCNodeDatabase {

    public static final String DB_PROPERTY = "kryo.dbFile";

    File dbFile;
    Task flusher;
    Map<String, Map<String, Object>> db = new HashMap<String, Map<String, Object>>();

    public boolean ramOnly() {
        return true;
    }

    @SuppressWarnings("unchecked")
    synchronized public void init(Properties props) {
        try {
            dbFile = new File(props.getProperty("kryo.dbFile"));
            if (dbFile.exists()) {
                DataInputStream dis = new DataInputStream(new FileInputStream(dbFile));
                byte[] data;
                dis.readFully(data = new byte[(int) dbFile.length()]);
                Input in = new Input(data);
                db = (Map<String, Map<String, Object>>) KryoLib.kryo().readClassAndObject(in);
                in.close();
                dis.close();
                System.err.printf("KryoFileDB: <%s> read: %s bytes, tables: %s\n", dbFile, in.total(), db.keySet());
                dbFile = new File(dbFile.getAbsolutePath() + ".new.db");
            } else {
                System.err.printf("Creating KryoFileDB: %s\n", dbFile.getAbsolutePath());
            }
        } catch (Exception x) {
            System.err.println("KryoFileDB init problem:" + x.getMessage());
        }
    }

    @Override
    public synchronized CRDTData<?> read(CRDTIdentifier id) {
        CRDTData<?> data = (CRDTData<?>) readSysData(id.getTable(), id.getKey());
        if (data != null)
            data.getCrdt().init(data.getId(), data.getClock(), data.getPruneClock(), true);

        return data;
    }

    @Override
    public synchronized boolean write(CRDTIdentifier id, CRDTData<?> data) {
        return writeSysData(id.getTable(), id.getKey(), data);
    }

    synchronized public Object readSysData(String table, String key) {
        Map<String, Object> tableData = db.get(table);
        return tableData == null ? null : tableData.get(key);
    }

    synchronized public boolean writeSysData(String table, String key, Object data) {
        Map<String, Object> tableData = db.get(table);
        if (tableData == null)
            db.put(table, tableData = new HashMap<String, Object>());
        tableData.put(key, data);

        // scheduleFlushToDisk();
        return true;
    }

    private void scheduleFlushToDisk() {
        if (flusher == null)
            flusher = new Task(1.0) {
                public void run() {
                    flushToDisk();
                }
            };
        else if (!flusher.isScheduled())
            flusher.reSchedule(1.0);
    }

    synchronized private void flushToDisk() {
        try {
            if (dbFile != null && db.size() > 0) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Output out = new Output(baos);
                KryoLib.kryo().writeClassAndObject(out, db);
                out.flush();
                out.close();
                baos.close();
                FileOutputStream fos = new FileOutputStream(dbFile);
                fos.write(baos.toByteArray(), 0, baos.size());
                fos.close();
                System.err.printf("KryoFileDB: <%s>, serialized: %s bytes to disk\n", dbFile, out.total());
            }
        } catch (Exception x) {
            // x.printStackTrace();
        }
    }
}
