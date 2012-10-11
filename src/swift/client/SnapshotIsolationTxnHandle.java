package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.TimestampMapping;
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
 * Implementation of {@link IsolationLevel#SNAPSHOT_ISOLATION} transaction,
 * which always read from a consistent snapshot.
 * 
 * @author mzawirski
 */
class SnapshotIsolationTxnHandle extends AbstractTxnHandle implements TxnHandle {
    final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectViewsCache;

    /**
     * @param manager
     *            manager maintaining this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param timestampMapping
     *            timestamp and timestamp mapping information used for all
     *            update of this transaction
     * @param snapshotClock
     *            clock representing committed update transactions visible to
     *            this transaction; left unmodified
     */
    SnapshotIsolationTxnHandle(final TxnManager manager, final CachePolicy cachePolicy,
            final TimestampMapping timestampMapping, final CausalityClock snapshotClock) {
        super(manager, cachePolicy, timestampMapping, snapshotClock);
        this.objectViewsCache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
    }

    @Override
    protected <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T getImpl(CRDTIdentifier id, boolean create,
            Class<V> classOfV, ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectViewsCache.get(id);
        if (localView == null) {
            localView = manager.getObjectVersionTxnView(this, id, getUpdatesDependencyClock(), create, classOfV,
                    updatesListener);
            objectViewsCache.put(id, localView);
        }
        return (T) localView;
    }
}
