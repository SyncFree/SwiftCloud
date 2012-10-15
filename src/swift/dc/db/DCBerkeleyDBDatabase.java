package swift.dc.db;

import static sys.net.api.Networking.Networking;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.db.*;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;
import swift.dc.DCConstants;

public class DCBerkeleyDBDatabase implements DCNodeDatabase {
    static Logger logger = Logger.getLogger(SwiftImpl.class.getName());
    Environment env;
    Map<String,Database> databases;
    File dir;
    TransactionConfig txnConfig;

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
            dir.mkdirs();

            EnvironmentConfig myEnvConfig = new EnvironmentConfig();
            myEnvConfig.setInitializeCache(true);
            myEnvConfig.setInitializeLocking(true);
            myEnvConfig.setInitializeLogging(true);
            myEnvConfig.setTransactional(true);
            // myEnvConfig.setMultiversion(true);
            myEnvConfig.setAllowCreate(true);

            env = new Environment(dir, myEnvConfig);

            txnConfig = new TransactionConfig();
            txnConfig.setSnapshot(true);

        } catch (FileNotFoundException e) {
            throw new RuntimeException("Cannot create databases", e);
        } catch (DatabaseException e) {
            throw new RuntimeException("Cannot create databases", e);
        } catch( Error e) {
            throw new RuntimeException("Cannot create databases - maybe you are forgetting to include library in path.\n" +
                                "Try something like: -Djava.library.path=lib/build_unix/.libs to java command", e);
            
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
                dbConfig.setType(DatabaseType.HASH);

                try {
                    db = env.openDatabase(null, // txn handle
                            tableName, // db file name
                            tableName, // db name
                            dbConfig);
                    databases.put(tableName, db);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Cannot create database : " + tableName, e);
                } catch (DatabaseException e) {
                    throw new RuntimeException("Cannot create database : " + tableName, e);
                }
            }
            return db;
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
                    tx.commitNoSync();
                } catch (DatabaseException e) {
                    logger.throwing("DCBerkeleyDBDatabase", "put", e);
                }
        }
    }

}
