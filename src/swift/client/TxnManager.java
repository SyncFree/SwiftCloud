package swift.client;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * TODO: document
 * 
 * @author mzawirski
 */
public interface TxnManager {
    AbstractTxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cp, boolean readOnly) throws NetworkException;

    <V extends CRDT<V>> TxnLocalCRDT<V> getObjectLatestVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CachePolicy cachePolicy, boolean create, Class<V> classOfV, final ObjectUpdatesListener updatesListener)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    <V extends CRDT<V>> TxnLocalCRDT<V> getObjectVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CausalityClock version, boolean create, Class<V> classOfV, ObjectUpdatesListener updatesListener)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    void discardTxn(AbstractTxnHandle txn);

    void commitTxn(AbstractTxnHandle txn);
}
