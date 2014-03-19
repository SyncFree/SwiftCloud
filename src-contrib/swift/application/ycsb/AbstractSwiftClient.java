package swift.application.ycsb;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public abstract class AbstractSwiftClient extends DB {
    public static final int ERROR_NETWORK = -1;
    public static final int ERROR_NOT_FOUND = -2;
    public static final int ERROR_WRONG_TYPE = -3;
    public static final int ERROR_PRUNING_RACE = -4;
    public static final int ERROR_UNSUPPORTED = -5;
    public static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.SNAPSHOT_ISOLATION;
    public static final CachePolicy DEFAULT_CACHE_POLICY = CachePolicy.CACHED;
    public static final ObjectUpdatesListener DEFAULT_NOTIFICATIONS_SUBSCRIBER = TxnHandle.UPDATES_SUBSCRIBER;
    public static final boolean DEFAULT_ASYNC_COMMIT = false;

    protected SwiftSession session;
    protected IsolationLevel isolationLevel = DEFAULT_ISOLATION_LEVEL;
    protected CachePolicy cachePolicy = DEFAULT_CACHE_POLICY;
    protected ObjectUpdatesListener notificationsSubscriber = DEFAULT_NOTIFICATIONS_SUBSCRIBER;
    protected boolean asyncCommit = DEFAULT_ASYNC_COMMIT;

    @Override
    public void init() throws DBException {
        super.init();
        Sys.init();

        final Properties props = getProperties();
        String hostname = props.getProperty("swift.hostname");
        if (hostname == null) {
            hostname = "localhost";
        }
        String portString = props.getProperty("swift.port");
        final int port;
        if (portString != null) {
            port = Integer.parseInt(portString);
        } else {
            port = DCConstants.SURROGATE_PORT;
        }

        // TODO: document properties
        if (props.getProperty("swift.isolationLevel") != null) {
            try {
                isolationLevel = IsolationLevel.valueOf(props.getProperty("swift.isolationLevel"));
            } catch (IllegalArgumentException x) {
                System.err.println("Could not recognized isolationLevel=" + props.getProperty("swift.isolationLevel"));
            }
        }
        if (props.getProperty("swift.cachePolicy") != null) {
            try {
                cachePolicy = CachePolicy.valueOf(props.getProperty("swift.cachePolicy"));
            } catch (IllegalArgumentException x) {
                System.err.println("Could not recognized cachePolicy=" + props.getProperty("swift.cachePolicy"));
            }
        }
        if (props.getProperty("swift.notifications") != null) {
            notificationsSubscriber = Boolean.getBoolean(props.getProperty("swift.notifications")) ? TxnHandle.UPDATES_SUBSCRIBER
                    : null;
        }
        if (props.getProperty("swift.asyncCommit") != null) {
            asyncCommit = Boolean.getBoolean(props.getProperty("swift.asyncCommit"));
        }

        final SwiftOptions options = new SwiftOptions(hostname, port, props);
        session = SwiftImpl.newSingleSessionInstance(options);
    }

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        if (session == null) {
            return;
        }
        session.stopScout(true);
        session = null;
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        TxnHandle txn = null;
        try {
            txn = session.beginTxn(isolationLevel, cachePolicy, true);
            int res = readImpl(txn, table, key, fields, result);
            if (res == 0) {
                txnCommit(txn);
            }
            return res;
        } catch (SwiftException x) {
            return handleException(x);
        } finally {
            tryTerminateTxn(txn);
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        return ERROR_UNSUPPORTED;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        TxnHandle txn = null;
        try {
            txn = session.beginTxn(isolationLevel, cachePolicy, false);
            int res = updateImpl(txn, table, key, values);
            if (res == 0) {
                txnCommit(txn);
            }
            return res;
        } catch (SwiftException x) {
            return handleException(x);
        } finally {
            tryTerminateTxn(txn);
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        TxnHandle txn = null;
        try {
            // WISHME: blind updates would help here
            txn = session.beginTxn(isolationLevel, cachePolicy, false);
            int res = insertImpl(txn, table, key, values);
            if (res == 0) {
                txnCommit(txn);
            }
            return res;
        } catch (SwiftException x) {
            return handleException(x);
        } finally {
            tryTerminateTxn(txn);
        }
    }

    @Override
    public int delete(String table, String key) {
        return ERROR_UNSUPPORTED;
    }

    protected abstract int readImpl(TxnHandle txn, String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

    protected abstract int updateImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    protected abstract int insertImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    private void tryTerminateTxn(TxnHandle txn) {
        if (txn != null && !txn.getStatus().isTerminated()) {
            txn.rollback();
        }
    }

    private void txnCommit(TxnHandle txn) {
        if (asyncCommit) {
            txn.commitAsync(null);
        } else {
            txn.commit();
        }
    }

    private int handleException(final SwiftException x) {
        x.printStackTrace();
        if (x instanceof WrongTypeException) {
            return ERROR_WRONG_TYPE;
        } else if (x instanceof NoSuchObjectException) {
            return ERROR_NOT_FOUND;
        } else if (x instanceof VersionNotFoundException) {
            return ERROR_PRUNING_RACE;
        } else if (x instanceof NetworkException) {
            return ERROR_NETWORK;
        } else {
            System.err.println("Unexepcted type of exception");
            return -666;
        }
    }
}
