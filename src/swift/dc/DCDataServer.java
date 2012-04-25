package swift.dc;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.client.proto.*;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.dc.db.DCNodeDatabase;
import swift.dc.proto.DHTExecCRDT;
import swift.dc.proto.DHTExecCRDTReply;
import swift.dc.proto.DHTExecCRDTReplyHandler;
import swift.dc.proto.DHTGetCRDT;
import swift.dc.proto.DHTGetCRDTReply;
import swift.dc.proto.DHTGetCRDTReplyHandler;
import swift.dc.proto.DHTSendNotification;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Connection;
import sys.dht.api.DHT.Key;
import sys.dht.api.StringKey;

/**
 * Class to maintain data in the server.
 * 
 * @author preguica
 */
class DCDataServer {

    Map<String, Map<String, CRDTData<?>>> db;
    Map<CRDTIdentifier, LockInfo> locks;

    CausalityClock version;
    String localSurrogateId;
    Observer localSurrogate;
    
    DCNodeDatabase dbServer;
    DHT dhtClient;

    LinkedList<NotificationRecord> notifications;

    Set<CRDTData<?>> modified;

    DCDataServer(DCSurrogate surrogate, Properties props) {
        initStore();
        initData(props);
        this.localSurrogate = new LocalObserver(surrogate);
        this.localSurrogateId = surrogate.getId();
        initDHT();
        Thread t = new Thread() {
            public void run() {
                for (;;) {
                    try {
                        NotificationRecord record = null;
                        synchronized (notifications) {
                            while (notifications.isEmpty())
                                try {
                                    notifications.wait();
                                } catch (InterruptedException e) {
                                    // do nothing
                                }
                            record = notifications.removeFirst();
                        }
                        if (record != null)
                            record.to.sendNotification(record.info);
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            }

        };
        t.setDaemon(true);
        t.start();
        DCConstants.DCLogger.info("Data server ready...");
    }
    
    /**
     * Start backgorund thread that dumps to disk
     */
    void initStore() {
        Thread t = new Thread() {
            public void run() {
                for (;;) {
                    try {
                        DCConstants.DCLogger.info("Dumping changed objects");
                        
                        List<CRDTData<?>> list = null;
                        synchronized( modified) {
                            list = new ArrayList<CRDTData<?>>( modified);
                        }
                        Iterator<CRDTData<?>> it = list.iterator();
                        while( it.hasNext()) {
                            CRDTData<?> obj = it.next();
                            lock( obj.id);
                            try {
                                modified.remove(obj);
                            } finally {
                                unlock( obj.id);
                            }
                            writeCRDTintoDB(obj);
                        }
                        
                        Thread.sleep(1000000);
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
            public void onReceive(Connection con, Key key, DHTGetCRDT request) {
                DCConstants.DCLogger.info( "DHT data server: get CRDT : " + request.getId());
                con.reply(new DHTGetCRDTReply( localGetCRDTObject( new RemoteObserver( request.getSurrogateId(), con), request.getId(), request.getSubscribe())));
            }

            @Override
            public void onReceive(Connection con, Key key, DHTExecCRDT request) {
                DCConstants.DCLogger.info( "DHT data server: exec CRDT : " + request.getGrp().getTargetUID());
                con.reply(new DHTExecCRDTReply( localExecCRDT( new RemoteObserver( request.getSurrogateId(), con), 
                        request.getGrp(), request.getSnapshotVersion(), request.getTrxVersion())));
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

    private void addNotification(NotificationRecord record) {
        synchronized (notifications) {
            notifications.addLast(record);
            notifications.notifyAll();
        }
    }

    private void initData( Properties props) {
        this.db = new HashMap<String, Map<String, CRDTData<?>>>();
        this.locks = new HashMap<CRDTIdentifier, LockInfo>();
        this.notifications = new LinkedList<NotificationRecord>();
        this.modified = new HashSet<CRDTData<?>>();

        this.version = ClockFactory.newClock();
        
        initDB( props);

        IntegerVersioned i = new IntegerVersioned();
        CRDTIdentifier id = new CRDTIdentifier("e", "1");
        i.init(id, version.clone(), version.clone(), true);
        localPutCRDT(localSurrogate, id, i, i.getClock(), i.getPruneClock());

        IntegerVersioned i2 = new IntegerVersioned();
        CRDTIdentifier id2 = new CRDTIdentifier("e", "2");
        i2.init(id2, version.clone(), version.clone(), true);
        localPutCRDT(localSurrogate, id2, i2, i2.getClock(), i2.getPruneClock());

    }
    
    /**********************************************************************************************
     * DATABASE FUNCTIONS
     *********************************************************************************************/
    void initDB( Properties props) {
        try {
            dbServer = (DCNodeDatabase)Class.forName( props.getProperty(DCConstants.DATABASE_CLASS)).newInstance();
        } catch (Exception e) {
            throw new RuntimeException( "Cannot start underlying database", e);
        }
        dbServer.init(props);
        
    }
    CRDTData<?> readCRDTFromDB( CRDTIdentifier id) {
        return (CRDTData<?>)dbServer.read( id);
    }

    void writeCRDTintoDB( CRDTData<?> data) {
    }
    /**
     * Returns database entry. If create, creates a new empty database entry.
     * It assumes that the given entry has been locked.
     * @param table
     * @param key
     * @param create
     * @return
     */
    CRDTData<?> getDatabaseEntry( CRDTIdentifier id) {
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
        data = readCRDTFromDB( id);
        synchronized(m) {
            if( data == null)
                data = new CRDTData( id);
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
    <V extends CRDT<V>> boolean execCRDT(CRDTObjectOperationsGroup<V> grp, CausalityClock snapshotVersion,
            CausalityClock trxVersion) {
        final StringKey key = new StringKey( grp.getTargetUID().toString());
        DHT_Node
        final Result<Boolean> result = new Result<Boolean>();
        while( ! result.hasResult()) {
            dhtClient.send( key, 
                new DHTExecCRDT( localSurrogateId, grp, snapshotVersion, trxVersion), new DHTExecCRDTReplyHandler() {
                    @Override
                    public void onReceive(DHTExecCRDTReply reply) {
                        result.setResult( reply.isResult());
                    }
            });
            result.waitForResult(2000);
            // TODO: probably should not continue forever !!! 
        }
        return result.getResult();
        //TODO: if object is local, just execute local object
//        return localExecCRDT(localSurrogate, grp, snapshotVersion, trxVersion);
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
    CRDTObject<?> getCRDT(CRDTIdentifier id, SubscriptionType subscribe) {
        final StringKey key = new StringKey( id.toString());
        final Result<CRDTObject<?>> result = new Result<CRDTObject<?>>();
        while( ! result.hasResult()) {
            dhtClient.send( key, 
                new DHTGetCRDT( localSurrogateId, id, subscribe), new DHTGetCRDTReplyHandler() {
                    @Override
                    public void onReceive(DHTGetCRDTReply reply) {
                        result.setResult( reply.getObject());
                    }
            });
            result.waitForResult(2000);
            // TODO: probably should not continue forever !!! 
        }
        return result.getResult();
        //TODO: if object is local, just execute local object
//        return localGetCRDTObject(localSurrogate, id, subscribe);
    }

    /**
     * Return null if CRDT does not exist
     */
    <V extends CRDT<V>> CRDTData<V> localPutCRDT(Observer observer, CRDTIdentifier id, CRDT<V> crdt,
            CausalityClock clk, CausalityClock prune) {
        lock(id);
        try {
            @SuppressWarnings("unchecked")
            CRDTData<V> data = (CRDTData<V>)this.getDatabaseEntry(id);
            if( data.empty) {
                data.initValue(crdt, clk, prune);
            } else {
                data.crdt.merge(crdt);
                data.clock.merge(clk);
                data.pruneClock.merge(prune);
            }
            setModifiedDatabaseEntry(data);
            return data;
        } finally {
            unlock(id);
        }
    }

    @SuppressWarnings("unchecked")
    <V extends CRDT<V>> boolean localExecCRDT(Observer observer, CRDTObjectOperationsGroup<V> grp,
            CausalityClock snapshotVersion, CausalityClock trxVersion) {
        CRDTIdentifier id = grp.getTargetUID();
        lock(id);
        try {
            CRDTData<?> data = localGetCRDT(observer, id, SubscriptionType.NONE);
            if (data == null) {
                if (!grp.hasCreationState()) {
                    return false;
                }
                CRDT crdt = grp.getCreationState();
                // TODO: check clocks
                CausalityClock clk = grp.getDependency();
                if (clk == null) {
                    clk = ClockFactory.newClock();
                }
                CausalityClock prune = ClockFactory.newClock();
                crdt.init(id, clk, prune, true);
                data = localPutCRDT(observer, id, crdt, clk, prune); // will merge if object exists
            }
            CausalityClock oldClock = data.clock.clone();

            // Assumption: dependencies are checked at sequencer level, since
            // causality and dependencies are given at inter-object level.
            data.crdt.execute((CRDTObjectOperationsGroup) grp, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
            data.clock = data.crdt.getClock();

            if (data.observers.size() > 0 || data.notifiers.size() > 0) {
                ObjectSubscriptionInfo info = new ObjectSubscriptionInfo(id, oldClock, data.clock.clone(), grp);
                Iterator<Observer> it = data.observers.iterator();
                while (it.hasNext()) {
                    Observer o = it.next();
                    addNotification(new NotificationRecord(o, info));
                }
                if (data.notifiers.size() > 0) {
                    info = info.clone();
                    info.clearOperations();
                    it = data.notifiers.iterator();
                    while (it.hasNext()) {
                        Observer o = it.next();
                        it.remove();
                        if (!data.observers.contains(o))
                            addNotification(new NotificationRecord(o, info));
                    }
                }
            }

            return true;
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
    CRDTObject<?> localGetCRDTObject(Observer observer, CRDTIdentifier id, SubscriptionType subscribe) {
        lock(id);
        try {
            CRDTData<?> data = localGetCRDT(observer, id, subscribe);
            if( data == null)
                return null;
            return new CRDTObject( data);
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
    CRDTData<?> localGetCRDT(Observer observer, CRDTIdentifier id, SubscriptionType subscribe) {
        lock(id);
        try {
            CRDTData<?> data = this.getDatabaseEntry(id);
            if( data.empty)
                return null;
            if (subscribe == SubscriptionType.UPDATES) {
                data.addObserver(observer);
            } else if (subscribe == SubscriptionType.NOTIFICATION) {
                data.addNotifier(observer);
            }

            return data;
        } finally {
            unlock(id);
        }
    }


}

// class ObserverRecord
// {
// Observer observer;
// boolean updates;
// boolean notifications;
//
// ObserverRecord( Observer observer, SubscriptionType type) {
// this.observer = observer;
// if( type == SubscriptionType.UPDATES)
// updates = true;
// else
// notifications = true;
// }
//
// public boolean isUpdates() {
// return updates;
// }
//
// public void setUpdates(boolean updates) {
// this.updates = updates;
// }
//
// public boolean isNotifications() {
// return notifications;
// }
//
// public void setNotifications(boolean notifications) {
// this.notifications = notifications;
// }
// }


interface Observer {
    public void sendNotification(ObjectSubscriptionInfo info);
}

class LocalObserver implements Observer {
    DCSurrogate surrogate; // replace with information to access the FCSurrogate

    LocalObserver(DCSurrogate s) {
        this.surrogate = s;
    }

    public void sendNotification(ObjectSubscriptionInfo info) {
        surrogate.notifyNewUpdates(info);
    }

    public int hashCode() {
        return surrogate.hashCode();
    }

    public boolean equals(Object o) {
        return (o instanceof Observer) && surrogate == ((LocalObserver) o).surrogate;
    }
}

class RemoteObserver implements Observer {
    String surrogateId; 
    Connection con;
    
    public RemoteObserver(String surrogateId, Connection con) {
        this.surrogateId = surrogateId;
        this.con = con;
    }

    public void sendNotification(ObjectSubscriptionInfo info) {
        //TODO: should this be a reply ?
        con.reply( new DHTSendNotification( info));
    }

    public int hashCode() {
        return surrogateId.hashCode();
    }

    public boolean equals(Object o) {
        return (o instanceof RemoteObserver) && surrogateId.equals(((RemoteObserver) o).surrogateId);
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

class NotificationRecord {
    Observer to;
    ObjectSubscriptionInfo info;

    NotificationRecord(Observer to, ObjectSubscriptionInfo info) {
        this.to = to;
        this.info = info;
    }
}

class Result<T>
{
    T obj;
    boolean hasResult;
    
    Result() {
        hasResult = false;
    }
    
    synchronized boolean hasResult() {
        return hasResult;
    }
    synchronized T getResult() {
        return obj;
    }
    synchronized void setResult( T o) {
        obj = o;
        hasResult = true;
        notifyAll();
        
    }
    synchronized boolean waitForResult( long timeout) {
        if( ! hasResult) {
            try {
                wait( timeout);
            } catch( Exception e) {
                // do nothing
            }
        }
        return hasResult;
    }
}