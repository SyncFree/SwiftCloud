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
package swift.application.adservice;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import swift.crdt.AddWinsSetCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.LowerBoundCounterCRDT;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.stats.Tally;

// implements the ad service functionality

public class SwiftAdservice {

    private static Logger logger = Logger.getLogger("swift.advertis");

    private SwiftSession server;
    private final IsolationLevel isolationLevel;
    private final CachePolicy cachePolicy;
    private final ObjectUpdatesListener updatesSubscriber;
    private final boolean asyncCommit;

    private String DCId;

    // Warning: workload is not deterministic!!
    private Random rg;

    public SwiftAdservice(SwiftSession clientServer, IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean subscribeUpdates, boolean asyncCommit, String DCId) {
        server = clientServer;
        this.isolationLevel = isolationLevel;
        this.cachePolicy = cachePolicy;
        this.updatesSubscriber = subscribeUpdates ? TxnHandle.UPDATES_SUBSCRIBER : null;
        this.asyncCommit = asyncCommit;
        this.DCId = DCId;
        this.rg = new Random();
    }

    public SwiftSession getSwift() {
        return server;
    }

    // FIXME Return error code?
    void addAd(final String adTitle, final String adCategory, final int maximumViews) {
        logger.info("Got add advertisement request for " + adTitle);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, CachePolicy.STRICTLY_MOST_RECENT, false);
            Ad newAd = addAd(txn, adTitle, adCategory, maximumViews);
            logger.info("Created Advertisement: " + newAd);
            // Here only synchronous commit, as otherwise the following tests
            // might fail.
            txn.commit();
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
    }

    protected Ad addAd(final TxnHandle txn, final String adTitle, final String category, final int maximumViews)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        LWWRegisterCRDT<Ad> reg = txn.get(NamingScheme.forAd(adTitle), true, LWWRegisterCRDT.class, null);
        Ad newAd = new Ad(adTitle, category, maximumViews);
        reg.set((Ad) newAd.copy());

        AddWinsSetCRDT<String> adSet = txn.get(NamingScheme.forCategory(reg.getValue().category), true,
                AddWinsSetCRDT.class, null);
        adSet.add(adTitle);

        LowerBoundCounterCRDT viewCount = txn
                .get(NamingScheme.forViewCount(adTitle), true, LowerBoundCounterCRDT.class);
        viewCount.increment(maximumViews, DCId);

        return newAd;
    }

    @SuppressWarnings("unchecked")
    public Integer viewAd(String category) {
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(isolationLevel, cachePolicy, false);
            AddWinsSetCRDT<String> adCategoryCRDT = txn.get(NamingScheme.forCategory(category), false,
                    AddWinsSetCRDT.class, updatesSubscriber);
            String[] adCategory = adCategoryCRDT.getValue().toArray(new String[0]);
            if (adCategory.length == 0) {
                logger.info("No more ads to display");
                return null;
            }
            int index = rg.nextInt(adCategory.length);
            LowerBoundCounterCRDT viewCount = get(txn, NamingScheme.forViewCount(adCategory[index]), false,
                    LowerBoundCounterCRDT.class);
            if (viewCount.getValue() > 0) {
                viewCount.decrement(1, DCId);
                logger.info("Views remaining: " + viewCount.getValue());
            } else {
                adCategoryCRDT.remove(adCategory[index]);
                logger.info("Cannot view ad");
            }
            commitTxn(txn);
            return viewCount.getValue();
        } catch (SwiftException e) {
            logger.warning(e.getMessage());
        } finally {
            if (txn != null && !txn.getStatus().isTerminated()) {
                txn.rollback();
            }
        }
        return null;

    }

    private void commitTxn(final TxnHandle txn) {
        if (asyncCommit) {
            txn.commitAsync(null);
        } else {
            txn.commit();
        }
    }

    Map<CRDTIdentifier, CRDT<?>> bulkRes = new HashMap<CRDTIdentifier, CRDT<?>>();

    void bulkGet(TxnHandle txn, CRDTIdentifier... ids) {
        txn.bulkGet(ids);
    }

    void bulkGet(TxnHandle txn, Set<CRDTIdentifier> ids) {
        txn.bulkGet(ids.toArray(new CRDTIdentifier[ids.size()]));
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> V get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        V res = (V) bulkRes.remove(id);
        if (res == null)
            res = txn.get(id, create, classOfV, updatesListener);
        return res;
    }

    Tally getLatency = new Tally("GetLatency");

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>, T extends CRDT<V>> T get(TxnHandle txn, CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

        T res = (T) bulkRes.remove(id);
        if (res == null)
            res = (T) txn.get(id, create, classOfT);

        return res;
    }
}
