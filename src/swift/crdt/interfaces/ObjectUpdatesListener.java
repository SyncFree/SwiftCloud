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
     * objects. Note that the completeness of these notifications is best-effort
     * only, i.e. an object might be updated in the store without notice.
     * 
     * TODO: discuss whether incompletness should remain here at API level or
     * shall we deal with it at the SwiftImpl level and offer completness here.
     * 
     * @param txn
     *            transaction where the old value of object has been read
     * @param id
     *            identifier of an object that has been externally updated
     */
    void onObjectUpdate(final TxnHandle txn, final CRDTIdentifier id);
}
