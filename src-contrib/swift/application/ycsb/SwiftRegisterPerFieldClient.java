package swift.application.ycsb;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import swift.crdt.AddWinsSetCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * SwiftCloud YCSB client based on Last Writer Wins register and Set. Each
 * fields is represented as LWW register and a set of fields for a key is
 * repesented as Add-Wins Set. Every operation runs within a single synchronous
 * transaction.
 * 
 * @author mzawirsk
 */
public class SwiftRegisterPerFieldClient extends AbstractSwiftClient {
    protected int readImpl(TxnHandle txn, String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        final HashMap<CRDTIdentifier, String> ids = getFieldIds(txn, table, key, fields, false);

        // Just trigger parallel reads.
        txn.bulkGet(ids.keySet(), null);
        // Then do separate reads, to trigger notifications too.
        for (final Entry<CRDTIdentifier, String> entry : ids.entrySet()) {
            @SuppressWarnings("unchecked")
            final LWWRegisterCRDT<String> register = (LWWRegisterCRDT<String>) txn.get(entry.getKey(), false,
                    LWWRegisterCRDT.class, notificationsSubscriber);
            final String value = register.getValue();
            if (value == null) {
                return ERROR_NOT_FOUND;
            }
            result.put(entry.getValue(), new StringByteIterator(register.getValue()));
        }
        return 0;
    }

    @Override
    protected int updateImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        final HashMap<CRDTIdentifier, String> ids = getFieldIds(txn, table, key, values.keySet(), true);

        // Just trigger parallel reads.
        txn.bulkGet(ids.keySet(), null);
        // Then do separate reads to trigger notifications too, and update.
        for (final Entry<CRDTIdentifier, String> entry : ids.entrySet()) {
            @SuppressWarnings("unchecked")
            final LWWRegisterCRDT<String> register = (LWWRegisterCRDT<String>) txn.get(entry.getKey(), true,
                    LWWRegisterCRDT.class, notificationsSubscriber);
            register.set(values.get(entry.getValue()).toString());
        }
        return 0;
    }

    @Override
    protected int insertImpl(TxnHandle txn, String table, String key, HashMap<String, ByteIterator> values)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        @SuppressWarnings("unchecked")
        AddWinsSetCRDT<String> fieldsInfo = txn.get(new CRDTIdentifier(table, key), true, AddWinsSetCRDT.class,
                notificationsSubscriber);
        for (final String field : values.keySet()) {
            fieldsInfo.add(field);
        }

        final HashMap<CRDTIdentifier, String> ids = getFieldIds(txn, table, key, values.keySet(), true);
        // Then do separate reads to trigger notifications too, and update.
        for (final Entry<CRDTIdentifier, String> entry : ids.entrySet()) {
            @SuppressWarnings("unchecked")
            final LWWRegisterCRDT<String> register = (LWWRegisterCRDT<String>) txn.get(entry.getKey(), true,
                    LWWRegisterCRDT.class, notificationsSubscriber);
            register.set(values.get(entry.getValue()).toString());
        }
        return 0;
    }

    protected HashMap<CRDTIdentifier, String> getFieldIds(TxnHandle txn, String table, String key, Set<String> fields,
            boolean create) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        if (fields == null) {
            // Retrieve information on fields.
            @SuppressWarnings("unchecked")
            final AddWinsSetCRDT<String> fieldsInfo = txn.get(new CRDTIdentifier(table, key), create,
                    AddWinsSetCRDT.class, notificationsSubscriber);
            fields = fieldsInfo.getValue();
        }
        final HashMap<CRDTIdentifier, String> ids = new HashMap<CRDTIdentifier, String>(fields.size(), 2.0f);
        for (final String field : fields) {
            ids.put(new CRDTIdentifier(table, key + "-" + field), field);
        }
        return ids;
    }
}
