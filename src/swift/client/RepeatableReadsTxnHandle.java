package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * Implementation of {@link IsolationLevel#REPEATABLE_READS} transaction, which
 * always read from a snapshot, possibly inconsistent and provides repeatable
 * reads.
 * <p>
 * It tries to offer the latest available object version, accessing the store
 * according to the {@link CachePolicy}.
 * 
 * @author mzawirski
 */
class RepeatableReadsTxnHandle extends AbstractTxnHandle implements TxnHandle {
    final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectViewsCache;

    /**
     * @param manager
     *            manager maintaining this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param localTimestamp
     *            local timestamp used for local operations of this transaction
     */
    RepeatableReadsTxnHandle(final TxnManager manager, final CachePolicy cachePolicy, final Timestamp localTimestamp) {
        super(manager, cachePolicy, localTimestamp);
        this.objectViewsCache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
    }

    @Override
    protected <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T getImpl(CRDTIdentifier id, boolean create,
            Class<V> classOfV, ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectViewsCache.get(id);
        if (localView == null) {
            localView = manager.getObjectLatestVersionTxnView(this, id, cachePolicy, create, classOfV, updatesListener);
            objectViewsCache.put(id, localView);
            updateUpdatesDependencyClock(localView.getClock());
        }
        return (T) localView;
    }
}
