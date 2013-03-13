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
package swift.dc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.dc.db.DCNodeDatabase;
import swift.proto.DHTExecCRDT;
import swift.proto.DHTExecCRDTReply;
import swift.proto.DHTGetCRDT;
import swift.proto.DHTGetCRDTReply;
import swift.proto.ObjectUpdatesInfo;
import swift.proto.SwiftProtocolHandler;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.Key;
import sys.dht.api.StringKey;
import sys.utils.Threading;

/**
 * Class to maintain data in the server.
 * 
 * @author preguica
 */
class DCDataServer {
    private static Logger logger = Logger.getLogger(DCDataServer.class.getName());

    Map<CRDTIdentifier, LockInfo> locks;
    Map<String, Map<String, CRDTData<?>>> db;

    CausalityClock version;
    CausalityClock cltClock;
    String localSurrogateId;

    DHT dhtClient;
    DCNodeDatabase dbServer;
    static public boolean prune;

    Set<CRDTData<?>> modified;

    DCDataServer(DCSurrogate surrogate, Properties props) {
        this.localSurrogateId = surrogate.getId();
        prune = Boolean.parseBoolean(props.getProperty(DCConstants.PRUNE_POLICY));
        initStore();
        initData(props);
        initDHT();
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Data server ready...");
        }
    }

    /**
     * Start backgorund thread that dumps to disk
     */
    void initStore() {
        Thread t = new Thread() {
            public void run() {
                for (;;) {
                    try {
                        List<CRDTData<?>> list = null;
                        synchronized (modified) {
                            list = new ArrayList<CRDTData<?>>(modified);
                        }
                        Iterator<CRDTData<?>> it = list.iterator();
                        while (it.hasNext()) {
                            CRDTData<?> obj = it.next();
                            lock(obj.id);
                            try {
                                synchronized (modified) {
                                    modified.remove(obj);
                                }
                            } finally {
                                unlock(obj.id);
                            }
                            writeCRDTintoDB(obj);
                        }

                        Thread.sleep(DCConstants.SYNC_PERIOD);
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            }

        };
        t.setDaemon(true);
        t.start();

    }

    /**
     * Start
     */
    void initDHT() {
        DHT_Node.start();

        dhtClient = DHT_Node.getStub();

        DHT_Node.setHandler(new DHTDataNode.RequestHandler() {

            @Override
            public void onReceive(Handle con, Key key, DHTGetCRDT request) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("DHT data server: get CRDT : " + request.getId());
                }
                con.reply(new DHTGetCRDTReply(localGetCRDTObject(request.getId(), request.getVersion(),
                        request.getCltId())));
            }

            @Override
            public void onReceive(Handle con, Key key, DHTExecCRDT request) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("DHT data server: exec CRDT : " + request.getGrp().getTargetUID());
                }
                con.reply(new DHTExecCRDTReply(localExecCRDT(request.getGrp(), request.getSnapshotVersion(),
                        request.getTrxVersion(), request.getTxTs(), request.getCltTs(), request.getPrvCltTs(),
                        request.getCurDCVersion())));
            }
        });
    }

    void lock(CRDTIdentifier id) {
        synchronized (locks) {
            LockInfo l = locks.get(id);
            if (l != null && l.ownedByMe()) {
                l.lock();
                return;
            }
            while (locks.containsKey(id)) {
                try {
                    locks.wait();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                }
            }
            locks.put(id, new LockInfo());
        }
    }

    void unlock(CRDTIdentifier id) {
        synchronized (locks) {
            LockInfo l = locks.get(id);
            if (l.unlock()) {
                locks.remove(id);
                locks.notifyAll();
            }
        }
    }

    /*
     * private void addNotification(NotificationRecord record) { synchronized
     * (notifications) { notifications.addLast(record);
     * notifications.notifyAll(); } }
     */
    private void initData(Properties props) {
        this.db = new HashMap<String, Map<String, CRDTData<?>>>();
        this.locks = new HashMap<CRDTIdentifier, LockInfo>();
        // this.notifications = new LinkedList<NotificationRecord>();
        this.modified = new HashSet<CRDTData<?>>();

        this.version = ClockFactory.newClock();
        this.cltClock = ClockFactory.newClock();

        initDB(props);

        if (dbServer.ramOnly()) {

            IntegerVersioned i = new IntegerVersioned();
            CRDTIdentifier id = new CRDTIdentifier("e", "1");
            i.init(id, version.clone(), version.clone(), true);
            localPutCRDT(id, i, i.getClock(), i.getPruneClock(), ClockFactory.newClock());

            IntegerVersioned i2 = new IntegerVersioned();
            CRDTIdentifier id2 = new CRDTIdentifier("e", "2");
            i2.init(id2, version.clone(), version.clone(), true);
            localPutCRDT(id2, i2, i2.getClock(), i2.getPruneClock(), ClockFactory.newClock());
        }
    }

    /**********************************************************************************************
     * DATABASE FUNCTIONS
     *********************************************************************************************/
    void initDB(Properties props) {
        try {
            dbServer = (DCNodeDatabase) Class.forName(props.getProperty(DCConstants.DATABASE_CLASS)).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Cannot start underlying database", e);
        }
        dbServer.init(props);

    }

    CRDTData<?> readCRDTFromDB(CRDTIdentifier id) {
        return (CRDTData<?>) dbServer.read(id);
    }

    void writeCRDTintoDB(CRDTData<?> data) {

        lock(data.id);
        try {
            dbServer.write(data.id, data);
        } finally {
            unlock(data.id);
        }
    }

    /**
     * Returns database entry. If create, creates a new empty database entry. It
     * assumes that the given entry has been locked.
     * 
     * @param table
     * @param key
     * @param create
     * @return
     */
    CRDTData<?> getDatabaseEntry(CRDTIdentifier id) {
        Map<String, CRDTData<?>> m;
        synchronized (db) {
            m = db.get(id.getTable());
            if (m == null) {
                m = new HashMap<String, CRDTData<?>>();
                db.put(id.getTable(), m);
            }
        }
        CRDTData<?> data = null;
        synchronized (m) {
            data = (CRDTData<?>) m.get(id.getKey());
            if (data != null)
                return data;
        }
        data = readCRDTFromDB(id);
        synchronized (m) {
            if (data == null)
                data = new CRDTData(id);
            m.put(id.getKey(), data);
            return data;
        }
    }

    private void setModifiedDatabaseEntry(CRDTData<?> crdt) {
        synchronized (modified) {
            modified.add(crdt);
        }
    }

    /**
     * Executes operations in the given CRDT
     * 
     * @return returns true if the operation could be executed.
     */
    <V extends CRDT<V>> ExecCRDTResult execCRDT(CRDTObjectUpdatesGroup<V> grp, CausalityClock snapshotVersion,
            CausalityClock trxVersion, Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs, CausalityClock curDCVersion) {
        final StringKey key = new StringKey(grp.getTargetUID().toString());
        if (!DHT_Node.getInstance().isHandledLocally(key)) {

            final AtomicReference<ExecCRDTResult> result = new AtomicReference<ExecCRDTResult>();

            for (;;) {
                dhtClient.send(key, new DHTExecCRDT(grp, snapshotVersion, trxVersion, txTs, cltTs, prvCltTs,
                        curDCVersion), new SwiftProtocolHandler() {
                    public void onReceive(DHTExecCRDTReply r) {
                        result.set(r.getResult());
                        Threading.synchronizedNotifyAllOn(result);
                    }
                });
                Threading.synchronizedWaitOn(result, 2000);

                if (result.get() != null)
                    return result.get();
            }
        } else
            return localExecCRDT(grp, snapshotVersion, trxVersion, txTs, cltTs, prvCltTs, curDCVersion);
    }

    // /**
    // * Return null if CRDT does not exist
    // */
    // <V extends CRDT<V>> CRDTData<V> putCRDT(CRDTIdentifier id, CRDT<V> crdt,
    // CausalityClock clk, CausalityClock prune) {
    // return localPutCRDT( localSurrogate, id, crdt, clk, prune);
    // }
    /**
     * Return null if CRDT does not exist
     * 
     * If clock equals to null, just return full CRDT
     * 
     * @param subscribe
     *            Subscription type
     * @return null if cannot fulfill request
     */
    CRDTObject getCRDT(CRDTIdentifier id, CausalityClock clk, String cltId) {
        final StringKey key = new StringKey(id.toString());
        if (!DHT_Node.getInstance().isHandledLocally(key)) {

            final AtomicReference<CRDTObject> result = new AtomicReference<CRDTObject>();
            for (;;) {
                dhtClient.send(key, new DHTGetCRDT(cltId, id, clk), new SwiftProtocolHandler() {
                    @Override
                    public void onReceive(DHTGetCRDTReply reply) {
                        result.set(reply.getObject());
                        Threading.synchronizedNotifyAllOn(result);
                    }
                });
                Threading.synchronizedWaitOn(result, 2000);

                if (result.get() != null)
                    return result.get();
            }
        } else
            return localGetCRDTObject(id, clk, cltId);
    }

    /**
     * Return null if CRDT does not exist
     */
    <V extends CRDT<V>> CRDTData<V> localPutCRDT(CRDTIdentifier id, CRDT<V> crdt, CausalityClock clk,
            CausalityClock prune, CausalityClock cltClock) {
        lock(id);
        try {
            @SuppressWarnings("unchecked")
            CRDTData<V> data = (CRDTData<V>) this.getDatabaseEntry(id);
            if (data.empty) {
                data.initValue(crdt, clk, prune, cltClock);
            } else {
                data.crdt.merge(crdt);
                // if (DCDataServer.prune) {
                // data.prunedCrdt.merge(crdt);
                // }
                data.clock.merge(clk);
                data.pruneClock.merge(prune);
                synchronized (this.cltClock) {
                    this.cltClock.merge(cltClock);
                }
            }
            setModifiedDatabaseEntry(data);
            return data;
        } finally {
            unlock(id);
        }
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> ExecCRDTResult localExecCRDT(CRDTObjectUpdatesGroup<V> grp, CausalityClock snapshotVersion,
            CausalityClock trxVersion, Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs, CausalityClock curDCVersion) {
        CRDTIdentifier id = grp.getTargetUID();
        lock(id);
        try {
            CRDTData<?> data = localGetCRDT(id);
            if (data == null) {
                if (!grp.hasCreationState()) {
                    System.err.println("OUCH!!!!!!!!!!!!!!!!" + grp.getTargetUID());
                    return new ExecCRDTResult(false);
                }
                CRDT<?> crdt = grp.getCreationState().copy();
                // TODO: check clocks
                CausalityClock clk = grp.getDependency();
                if (clk == null) {
                    clk = ClockFactory.newClock();
                } else {
                    clk = clk.clone();
                }
                CausalityClock prune = ClockFactory.newClock();
                CausalityClock cltClock = ClockFactory.newClock();
                crdt.init(id, clk, prune, true);
                data = localPutCRDT(id, crdt, clk, prune, cltClock); // will
                // merge if
                // object
                // exists
            }
            data.pruneIfPossible();
            CausalityClock oldClock = data.clock.clone();

            // crdt.augumentWithScoutClock(new Timestamp(clientId, clientTxs))
            // //
            // ensures that execute() has enough information to ensure tx
            // idempotence
            // crdt.execute(updates...)
            // crdt.discardScoutClock(clientId) // critical to not polute all
            // data
            // nodes and objects with big vectors, unless we want to do it until
            // pruning

            if (prvCltTs != null)
                data.crdt.augmentWithScoutClock(prvCltTs);

            // Assumption: dependencies are checked at sequencer level, since
            // causality and dependencies are given at inter-object level.
            data.crdt.execute((CRDTObjectUpdatesGroup) grp, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
            data.crdt.augmentWithDCClock(curDCVersion);

            /*
             * if (DCDataServer.prune) { if (prvCltTs != null)
             * data.prunedCrdt.augmentWithScoutClock(prvCltTs);
             * data.prunedCrdt.execute((CRDTObjectUpdatesGroup) grp,
             * CRDTOperationDependencyPolicy.RECORD_BLINDLY);
             * data.prunedCrdt.augmentWithDCClock(curDCVersion);
             * data.prunedCrdt.prune(data.clock, false);
             * data.prunedCrdt.discardScoutClock(cltTs.getIdentifier());
             * data.pruneClock = data.clock; }
             */
            data.crdt.discardScoutClock(cltTs.getIdentifier());
            data.clock = data.crdt.getClock();

            setModifiedDatabaseEntry(data);

            if (logger.isLoggable(Level.INFO)) {
                logger.info("Data Server: for crdt : " + data.id + "; clk = " + data.clock + " ; cltClock = "
                        + cltClock + ";  snapshotVersion = " + snapshotVersion + "; cltTs = " + cltTs);
            }
            synchronized (this.cltClock) {
                this.cltClock.recordAllUntil(cltTs);
            }
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Data Server: for crdt : " + data.id + "; clk = " + data.clock + " ; cltClock = "
                        + cltClock + ";  snapshotVersion = " + snapshotVersion + "; cltTs = " + cltTs);
            }
            return new ExecCRDTResult(true, grp.getTargetUID(), new ObjectUpdatesInfo(id, oldClock, data.clock.clone(),
                    data.pruneClock.clone(), grp));
        } finally {
            unlock(id);
        }

    }

    /**
     * Return null if CRDT does not exist
     * 
     * If clock equals to null, just return full CRDT
     * 
     * @param subscribe
     *            Subscription type
     * @return null if cannot fulfill request
     */
    CRDTObject localGetCRDTObject(CRDTIdentifier id, CausalityClock version, String cltId) {
        lock(id);
        try {
            CRDTData<?> data = localGetCRDT(id);
            if (data == null)
                return null;

            return new CRDTObject(data, version, cltId, cltClock);
        } finally {
            unlock(id);
        }
    }

    /**
     * Return null if CRDT does not exist
     * 
     * If clock equals to null, just return full CRDT
     * 
     * @param subscribe
     *            Subscription type
     * @return null if cannot fulfill request
     */
    CRDTData<?> localGetCRDT(CRDTIdentifier id) {
        lock(id);
        try {
            CRDTData<?> data = this.getDatabaseEntry(id);
            if (data.empty)
                return null;

            return data;
        } finally {
            unlock(id);
        }
    }

}

class LockInfo {
    Thread thread;
    int count;

    LockInfo() {
        thread = Thread.currentThread();
        count = 1;
    }

    boolean ownedByMe() {
        return thread.equals(Thread.currentThread());
    }

    void lock() {
        count++;
    }

    boolean unlock() {
        if (count > 0)
            count--;
        return count <= 0;
    }

}