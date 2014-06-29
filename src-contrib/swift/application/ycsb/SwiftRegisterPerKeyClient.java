package swift.application.ycsb;

import java.util.HashMap;
import java.util.Set;

import swift.crdt.LWWStringMapRegisterCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * SwiftCloud YCSB client based on Last Writer Wins register. Key with fields is
 * stored as a map inside a LWW register.
 * 
 * @author mzawirsk
 */
public class SwiftRegisterPerKeyClient extends AbstractSwiftClient {
    protected int readImpl(TxnHandle txn, String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        final LWWStringMapRegisterCRDT register = txn.get(new CRDTIdentifier(table, key), false,
                LWWStringMapRegisterCRDT.class, notificationsSubscriber);
        final HashMap<String, String> value = register.getValue();
        if (value == null) {
            return ERROR_NOT_FOUND;
        }
        if (fields == null) {
            StringByteIterator.putAllAsByteIterators(result, value);
        } else {
            for (final String field : fields) {
                result.put(field, new StringByteIterator(value.get(field)));
            }
        }
        return 0;
    }

    @Override
    protected int updateImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        final LWWStringMapRegisterCRDT register = txn.get(new CRDTIdentifier(table, key), false,
                LWWStringMapRegisterCRDT.class, notificationsSubscriber);
        final HashMap<String, String> value = register.getValue();
        if (value == null) {
            return ERROR_NOT_FOUND;
        }
        final HashMap<String, String> mutableValue = new HashMap<String, String>(value);
        StringByteIterator.putAllAsStrings(mutableValue, values);
        register.set(mutableValue);

        return 0;
    }

    @Override
    protected int insertImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        final LWWStringMapRegisterCRDT register = txn.get(new CRDTIdentifier(table, key), true,
                LWWStringMapRegisterCRDT.class, notificationsSubscriber);
        final HashMap<String, String> value = StringByteIterator.getStringMap(values);
        register.set(value);

        return 0;
    }
}
