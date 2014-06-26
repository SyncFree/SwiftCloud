package swift.application.ycsb;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import swift.crdt.AbstractPutOnlyLWWMapCRDT;
import swift.crdt.PutOnlyLWWStringMapCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * SwiftCloud YCSB client based on Last Writer Wins Map. Each YCSB key
 * translates into a put-only Map CRDT with LWW as a conflict resolution,
 * whereas each field translates into an entry in a map.
 * 
 * @author mzawirsk
 */
public class SwiftMapPerKeyClient extends AbstractSwiftClient {
    protected int readImpl(TxnHandle txn, String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        final PutOnlyLWWStringMapCRDT map = txn.get(new CRDTIdentifier(table, key), false,
                PutOnlyLWWStringMapCRDT.class, notificationsSubscriber);
        if (fields == null) {
            final HashMap<String, String> value = map.getValue();
            StringByteIterator.putAllAsByteIterators(result, value);
        } else {
            for (final String field : fields) {
                final String keyValue = map.get(field);
                if (keyValue == null) {
                    return ERROR_NOT_FOUND;
                }
                result.put(field, new StringByteIterator(keyValue));
            }
        }
        return 0;
    }

    @Override
    protected int updateImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        final PutOnlyLWWStringMapCRDT map = txn.get(new CRDTIdentifier(table, key), false,
                PutOnlyLWWStringMapCRDT.class, notificationsSubscriber);
        for (final Entry<String, ByteIterator> keyValue : values.entrySet()) {
            map.put(keyValue.getKey(), keyValue.getValue().toString());
        }
        return 0;
    }

    @Override
    protected int insertImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        final PutOnlyLWWStringMapCRDT map = txn.get(new CRDTIdentifier(table, key), true,
                PutOnlyLWWStringMapCRDT.class, notificationsSubscriber);
        for (final Entry<String, ByteIterator> keyValue : values.entrySet()) {
            map.put(keyValue.getKey(), keyValue.getValue().toString());
        }
        return 0;
    }
}
