/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import swift.clocks.CausalityClock;
import swift.clocks.TimestampMapping;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.TransactionsLog;
import sys.stats.Stats;

/**
 * Implementation of {@link IsolationLevel#SNAPSHOT_ISOLATION} transaction,
 * which always read from a consistent snapshot.
 * 
 * @author mzawirski
 */
class SnapshotIsolationTxnHandle extends AbstractTxnHandle implements TxnHandle {
    final Map<CRDTIdentifier, CRDT<?>> objectViewsCache;

    /**
     * Creates update transaction.
     * 
     * @param manager
     *            manager maintaining this transaction
     * @param sessionId
     *            id of the client session issuing this transaction
     * @param durableLog
     *            durable log for recovery
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param timestampMapping
     *            timestamp and timestamp mapping information used for all
     *            update of this transaction
     * @param snapshotClock
     *            clock representing committed update transactions visible to
     *            this transaction; left unmodified
     */
    SnapshotIsolationTxnHandle(final TxnManager manager, final String sessionId, final TransactionsLog durableLog,
            final CachePolicy cachePolicy, final TimestampMapping timestampMapping, final CausalityClock snapshotClock,
            Stats statistics) {
        super(manager, sessionId, durableLog, IsolationLevel.SNAPSHOT_ISOLATION, cachePolicy, timestampMapping,
                statistics);
        this.objectViewsCache = new ConcurrentHashMap<CRDTIdentifier, CRDT<?>>();
        updateUpdatesDependencyClock(snapshotClock);
    }

    /**
     * Creates read-only transaction.
     * 
     * @param manager
     *            manager maintaining this transaction
     * @param sessionId
     *            id of the client session issuing this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param snapshotClock
     *            clock representing committed update transactions visible to
     *            this transaction; left unmodified
     */
    SnapshotIsolationTxnHandle(final TxnManager manager, final String sessionId, final CachePolicy cachePolicy,
            final CausalityClock snapshotClock, Stats stats) {
        super(manager, sessionId, IsolationLevel.SNAPSHOT_ISOLATION, cachePolicy, stats);
        this.objectViewsCache = new ConcurrentHashMap<CRDTIdentifier, CRDT<?>>();
        updateUpdatesDependencyClock(snapshotClock);
    }

    @Override
    protected <V extends CRDT<V>> V getImpl(CRDTIdentifier id, boolean create, Class<V> classOfV,
            ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        CRDT<V> localView = (CRDT<V>) objectViewsCache.get(id);
        if (localView != null && updatesListener != null) {
            // force another read to install the listener and discard it
            manager.getObjectVersionTxnView(this, id, localView.getClock(), create, classOfV, updatesListener);
        }
        if (localView == null) {
            localView = manager.getObjectVersionTxnView(this, id, getUpdatesDependencyClock().clone(), create, classOfV,
                    updatesListener);
            objectViewsCache.put(id, localView);
        }
        return (V) localView;
    }
}
