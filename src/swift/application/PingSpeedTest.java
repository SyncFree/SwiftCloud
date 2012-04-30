package swift.application;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.NanoTimeCollector;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class PingSpeedTest {
    private static String sequencerName = "localhost";
    private static String dcName = "localhost";
    static int iterations = 200000;
    static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;
    static CachePolicy cachePolicy = CachePolicy.CACHED;
    static boolean notifications = false;
    static ObjectUpdatesListener uplistener = null;
    static volatile boolean wait = true;

    public static void main(String[] args) {
        System.out.println("PingSpeedTest start!");
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        // start DC server
        DCServer.main(new String[] { dcName });

        if (true) {
            uplistener = new DummyObjectUpdatesListener(Thread.currentThread());
        }

        Thread client1 = new Thread("client1") {
            public void run() {
                Sys.init();
                SwiftImpl clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
                client1Code(clientServer);
                clientServer.stop(true);
            }
        };
        client1.start();

        Thread client2 = new Thread("client2") {
            public void run() {
                Sys.init();
                SwiftImpl clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
                client2Code(clientServer);
                clientServer.stop(true);
            }
        };
        client2.start();
    }

    protected static void client1Code(SwiftImpl server) {
        try {
            System.out.println("Ping time");
            NanoTimeCollector timer = new NanoTimeCollector();
            timer.start();
            TxnHandle handle = server.beginTxn(isolationLevel, cachePolicy, false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), true, swift.crdt.IntegerVersioned.class,
                    uplistener);
            i1.add(1);
            handle.commit();
            int expected = 2;

            while (true) {
                if (notifications) {
                    while (wait) {
                    }
                }
                wait = false;
                TxnHandle txn = server.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i = txn.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class,
                        uplistener);
                if (expected == i.getValue()) {
                    long pingTime = timer.stop();
                    txn.commit();
                    System.out.println(pingTime);

                    if (expected / 2 < iterations) {
                        // wait for the system to settle down and finish
                        // internals
                        Thread.sleep(1000);
                        expected += 2;
                        timer.start();
                        increment(server);
                    } else {
                        break;
                    }
                } else {
                    // System.out.println("Value " + i.getValue());
                    txn.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void increment(SwiftImpl server) throws NetworkException, WrongTypeException,
            NoSuchObjectException, VersionNotFoundException {
        TxnHandle handle = server.beginTxn(isolationLevel, cachePolicy, false);
        IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class,
                uplistener);
        i1.add(1);
        handle.commit();
    }

    protected static void client2Code(SwiftImpl server) {
        try {
            int expected = 1;
            while (true) {
                TxnHandle handle = server.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class,
                        uplistener);
                if (i1.getValue() == expected) {
                    i1.add(1);
                    handle.commit();
                    if (expected / 2 < iterations - 1) {
                        // wait for the system to settle down and finish
                        // internals
                        expected += 2;
                    } else {
                        break;
                    }
                } else {
                    handle.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class DummyObjectUpdatesListener extends AbstractObjectUpdatesListener {
        Thread application;

        DummyObjectUpdatesListener(Thread app) {
            this.application = app;
        }

        @Override
        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            wait = false;
        }
    }
}
