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

import static sys.net.api.Networking.Networking;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.crdt.core.CRDTIdentifier;
import swift.dc.CRDTData;
import swift.dc.DCConstants;
import sys.utils.FileUtils;
import sys.utils.Props;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class DCBerkeleyDBDatabase implements DCNodeDatabase {
    static Logger logger = Logger.getLogger(DCBerkeleyDBDatabase.class.getName());
    Environment env;
    Map<String, Database> databases;
    File dir;
    TransactionConfig txnConfig;
    private boolean syncCommit;

    public DCBerkeleyDBDatabase() {
    }

    /*
     * DatabaseConfig dbConfig = new DatabaseConfig();
     * dbConfig.setTransactional(true); dbConfig.setAllowCreate(true);
     * dbConfig.setType(DatabaseType.BTREE);
     * 
     * Iterator<KVExecutionPolicy> it = config.getPolicies().iterator(); while(
     * it.hasNext()) { KVExecutionPolicy pol = it.next(); String tableName =
     * pol.getTableName();
     * 
     * Database db = env.openDatabase(null, // txn handle tableName, // db file
     * name tableName, // db name dbConfig);
     */

    @Override
    public void init(Properties props) {
        try {
            databases = new HashMap<String, Database>();

            String dirPath = props.getProperty(DCConstants.BERKELEYDB_DIR);

            dir = new File(dirPath);
            if (dir.exists())
                FileUtils.deleteDir(dir);
            dir.mkdirs();

            String restoreDBdir = Props.get(props, "restore_db");
            if (!restoreDBdir.isEmpty()) {
                FileUtils.copyDir(new File(dir.getParent() + File.separatorChar + restoreDBdir), dir);
            }

            syncCommit = Props.boolValue(props, "sync_commit", false);

            EnvironmentConfig myEnvConfig = new EnvironmentConfig();
            // myEnvConfig.setInitializeCache(true); // smd: not supported in
            // the JE edition...
            // myEnvConfig.setInitializeLocking(true);
            // myEnvConfig.setInitializeLogging(true);

            myEnvConfig.setCacheSize(100L << 20);
            myEnvConfig.setTransactional(true);
            // myEnvConfig.setMultiversion(true);
            myEnvConfig.setAllowCreate(true);

            env = new Environment(dir, myEnvConfig);

            EnvironmentMutableConfig mut = new EnvironmentMutableConfig();
            mut.setTxnWriteNoSyncVoid(true);

            env.setMutableConfig(mut);
            txnConfig = new TransactionConfig();
            // txnConfig.setSnapshot(true); //not available in the JE edition...

        } catch (DatabaseException e) {
            throw new RuntimeException("Cannot create databases", e);
        } catch (Error e) {
            throw new RuntimeException(
                    "Cannot create databases - maybe you are forgetting to include library in path.\n"
                            + "Try something like: -Djava.library.path=lib/build_unix/.libs to java command", e);

        }
    }

    private Database getDatabase(String tableName) {
        Database db = null;
        synchronized (databases) {
            db = databases.get(tableName);
            if (db == null) {
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setTransactional(true);
                dbConfig.setAllowCreate(true);
                // dbConfig.setType(DatabaseType.HASH);

                try {
                    db = env.openDatabase(null, // txn handle
                            tableName, // db file name
                            dbConfig);
                    databases.put(tableName, db);
                } catch (DatabaseException e) {
                    throw new RuntimeException("Cannot create database : " + tableName, e);
                }
            }
            return db;
        }
    }

    @Override
    public synchronized CRDTData<?> read(CRDTIdentifier id) {
        return (CRDTData<?>) readSysData(id.getTable(), id.getKey());
    }

    @Override
    public synchronized boolean write(CRDTIdentifier id, CRDTData<?> data) {
        return writeSysData(id.getTable(), id.getKey(), data);
    }

    @Override
    public boolean ramOnly() {
        return env == null;
    }

    @Override
    public synchronized Object readSysData(String table, String key) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("DCBerkeleyDBDatabase: get: " + table + ";" + key);
        }
        Database db = getDatabase(table);

        Transaction tx = null;
        try {
            tx = env.beginTransaction(null, txnConfig);

            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();

            // Perform the get.
            if (db.get(tx, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                return Networking.serializer().readObject(theData.getData());
            } else
                return null;
        } catch (IOException e) {
            logger.throwing("DCBerkeleyDBDatabase", "get", e);
            return null;
        } catch (DatabaseException e) {
            logger.throwing("DCBerkeleyDBDatabase", "get", e);
            return null;
        } finally {
            if (tx != null)
                try {
                    tx.commitNoSync();
                } catch (DatabaseException e) {
                    logger.throwing("DCBerkeleyDBDatabase", "get", e);
                }
        }
    }

    @Override
    public synchronized boolean writeSysData(String table, String key, Object data) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("DCBerkeleyDBDatabase: put: " + table + ";" + key);
        }
        Database db = getDatabase(table);

        Transaction tx = null;
        try {
            tx = env.beginTransaction(null, txnConfig);

            byte[] arr = Networking.serializer().writeObject(data);

            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry(arr);

            // Perform the put
            if (db.put(tx, theKey, theData) == OperationStatus.SUCCESS) {
                return true;
            } else
                return false;
        } catch (IOException e) {
            logger.throwing("DCBerkeleyDBDatabase", "put", e);
            return false;
        } catch (DatabaseException e) {
            logger.throwing("DCBerkeleyDBDatabase", "put", e);
            return false;
        } finally {
            if (tx != null)
                try {
                    if (syncCommit)
                        tx.commitSync();
                    else
                        tx.commitNoSync();
                } catch (DatabaseException e) {
                    logger.throwing("DCBerkeleyDBDatabase", "put", e);
                }
        }
    }

    public void restoreDB() {

    }

    @Override
    public void sync(boolean sync) {
        this.syncCommit = sync;
    }
}
