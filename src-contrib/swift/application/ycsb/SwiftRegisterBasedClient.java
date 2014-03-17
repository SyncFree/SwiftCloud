package swift.application.ycsb;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.RegisterCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * SwiftCloud YCSB client based on Last Writer Wins register. Key with fields is
 * stored as a map inside a LWW register. Every operation runs within a single
 * transaction and update subscription is triggered by reads.
 * 
 * @author mzawirsk
 */
public class SwiftRegisterBasedClient extends DB {
    public static final int ERROR_NETWORK = -1;
    public static final int ERROR_NOT_FOUND = -2;
    public static final int ERROR_WRONG_TYPE = -3;
    public static final int ERROR_PRUNING_RACE = -4;
    public static final int ERROR_UNSUPPORTED = -5;

    private SwiftSession session;
    private IsolationLevel isolationLevel;
    private CachePolicy cachePolicy;
    private ObjectUpdatesListener notificationsSubscriber;

    @Override
    public void init() throws DBException {
        super.init();
        Sys.init();

        final Properties props = getProperties();
        String hostname = props.getProperty("swiftcloud.hostname");
        if (hostname == null) {
            hostname = "localhost";
        }
        String portString = props.getProperty("swiftcloud.port");
        final int port;
        if (portString != null) {
            port = Integer.parseInt(portString);
        } else {
            port = DCConstants.SURROGATE_PORT;
        }
        // FIXME: provide as options?
        isolationLevel = IsolationLevel.SNAPSHOT_ISOLATION;
        cachePolicy = CachePolicy.STRICTLY_MOST_RECENT;
        notificationsSubscriber = TxnHandle.UPDATES_SUBSCRIBER;

        final SwiftOptions options = new SwiftOptions(hostname, port);
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
            @SuppressWarnings("unchecked")
            final RegisterCRDT<StringHashMapWrapper> register = (RegisterCRDT<StringHashMapWrapper>) txn.get(
                    new CRDTIdentifier(table, key), false, RegisterCRDT.class, notificationsSubscriber);
            final StringHashMapWrapper value = register.getValue();
            if (value == null) {
                return ERROR_NOT_FOUND;
            }
            if (fields == null) {
                StringByteIterator.putAllAsByteIterators(result, value.getValue());
            } else {
                for (final String field : fields) {
                    result.put(field, new StringByteIterator(value.getValue().get(field)));
                }
            }
            txn.commit();
            txn = null;
            return 0;
        } catch (WrongTypeException e) {
            return ERROR_WRONG_TYPE;
        } catch (NoSuchObjectException e) {
            return ERROR_NOT_FOUND;
        } catch (VersionNotFoundException e) {
            return ERROR_PRUNING_RACE;
        } catch (NetworkException e) {
            return ERROR_NETWORK;
        } finally {
            if (txn != null) {
                txn.rollback();
            }
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

            @SuppressWarnings("unchecked")
            final RegisterCRDT<StringHashMapWrapper> register = (RegisterCRDT<StringHashMapWrapper>) txn.get(
                    new CRDTIdentifier(table, key), false, RegisterCRDT.class, notificationsSubscriber);
            final StringHashMapWrapper value = register.getValue();
            if (value == null) {
                return ERROR_NOT_FOUND;
            }
            final StringHashMapWrapper mutableValue = (StringHashMapWrapper) value.copy();
            StringByteIterator.putAllAsStrings(mutableValue.getValue(), values);
            register.set(mutableValue);

            txn.commit();
            txn = null;
            return 0;
        } catch (WrongTypeException e) {
            return ERROR_WRONG_TYPE;
        } catch (NoSuchObjectException e) {
            return ERROR_NOT_FOUND;
        } catch (VersionNotFoundException e) {
            return ERROR_PRUNING_RACE;
        } catch (NetworkException e) {
            return ERROR_NETWORK;
        } finally {
            if (txn != null) {
                txn.rollback();
            }
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        TxnHandle txn = null;
        try {
            txn = session.beginTxn(isolationLevel, cachePolicy, false);

            @SuppressWarnings("unchecked")
            final RegisterCRDT<StringHashMapWrapper> register = (RegisterCRDT<StringHashMapWrapper>) txn.get(
                    new CRDTIdentifier(table, key), true, RegisterCRDT.class, notificationsSubscriber);
            final StringHashMapWrapper value = StringHashMapWrapper.createWithValue(StringByteIterator
                    .getStringMap(values));
            register.set(value);

            txn.commit();
            txn = null;
            return 0;
        } catch (WrongTypeException e) {
            return ERROR_WRONG_TYPE;
        } catch (NoSuchObjectException e) {
            return ERROR_NOT_FOUND;
        } catch (VersionNotFoundException e) {
            return ERROR_PRUNING_RACE;
        } catch (NetworkException e) {
            return ERROR_NETWORK;
        } finally {
            if (txn != null) {
                txn.rollback();
            }
        }
    }

    @Override
    public int delete(String table, String key) {
        return ERROR_UNSUPPORTED;
    }

}
