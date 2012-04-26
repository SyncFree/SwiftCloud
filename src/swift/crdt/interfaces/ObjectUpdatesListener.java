package swift.crdt.interfaces;

import swift.crdt.CRDTIdentifier;

/**
 * Listener interface defining notifications for application on object updates.
 * 
 * @author mzawirski
 */
public interface ObjectUpdatesListener {
    /**
     * Fired when external update has been observed on an object previously read
     * by the client transaction. This notification is fired at most once per
     * registered listener for an object read. Listener instances can be reused
     * between subsequent reads of an object, or between reads of different
     * objects.
     * 
     * @param txn
     *            transaction where the old value of object has been read
     * @param id
     *            identifier of an object that has been externally updated
     * @param previousValue
     *            previous value of an object
     */
    void onObjectUpdate(final TxnHandle txn, final CRDTIdentifier id, final TxnLocalCRDT<?> previousValue);
}
