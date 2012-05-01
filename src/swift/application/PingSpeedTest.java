package swift.application;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
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
    static int iterations = 10;
    static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;
    static CachePolicy cachePolicy = CachePolicy.CACHED;
    static boolean notifications = true;
    static CRDTIdentifier j = new CRDTIdentifier("e", "1");

    public static void main(String[] args) {
        System.out.println("PingSpeedTest start!");
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        // start DC server
        DCServer.main(new String[] { dcName });

        Thread client1 = new Thread("client1") {
            public void run() {
                Sys.init();
                SwiftImpl clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
                if (notifications) {
                    client1CodeNotifications(clientServer);
                } else {
                    client1Code(clientServer);
                }

            }
        };
        client1.start();

        Thread client2 = new Thread("client2") {
            public void run() {
                Sys.init();
                SwiftImpl clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
                if (notifications) {
                    client2CodeNotifications(clientServer);
                } else {
                    client2Code(clientServer);
                }
            }
        };
        client2.start();
    }

    protected static void client1Code(SwiftImpl swift) {
        try {
            System.out.println("Ping time");
            NanoTimeCollector timer = new NanoTimeCollector();

            timer.start();
            TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
            IntegerTxnLocal i1 = handle.get(j, true, swift.crdt.IntegerVersioned.class);
            i1.add(1);
            handle.commit();
            int expected = 2;

            while (true) {
                TxnHandle txn = swift.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i = txn.get(j, false, swift.crdt.IntegerVersioned.class);
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
                        increment(swift);
                    } else {
                        break;
                    }
                } else {
                    // System.out.println("Value " + i.getValue());
                    txn.rollback();
                }
            }
            swift.stop(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void client2Code(SwiftImpl swift) {
        try {
            int expected = 1;
            while (true) {
                TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class);
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

    protected static void increment(SwiftImpl swift) throws NetworkException, WrongTypeException,
            NoSuchObjectException, VersionNotFoundException {
        TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
        IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class);
        i1.add(1);
        handle.commit();
    }

    protected static void client1CodeNotifications(SwiftImpl swift) {
        try {
            System.out.println("Ping time");
            NanoTimeCollector timer = new NanoTimeCollector();

            timer.start();
            TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
            IntegerTxnLocal i1 = handle.get(j, true, swift.crdt.IntegerVersioned.class, new ObjectUpdatesListenerIncr1(
                    2, swift, timer));
            i1.add(1);
            handle.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void client2CodeNotifications(SwiftImpl swift) {
        try {
            int expected = 1;
            TxnHandle handle = swift.beginTxn(isolationLevel, CachePolicy.MOST_RECENT, false);
            IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class,
                    new ObjectUpdatesListenerIncr2(1, swift));
            if (i1.getValue() == expected) {
                i1.add(1);
                handle.commit();
                if (expected / 2 < iterations - 1) {
                    // wait for the system to settle down and finish
                    // internals
                    expected += 2;
                } else {
                    return;
                }
            } else {
                handle.rollback();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class ObjectUpdatesListenerIncr1 extends AbstractObjectUpdatesListener {
        int expected;
        SwiftImpl swift;
        NanoTimeCollector timer;

        ObjectUpdatesListenerIncr1(int expected, SwiftImpl swift, NanoTimeCollector timer) {
            this.expected = expected;
            this.swift = swift;
            this.timer = timer;
        }

        @Override
        public void onObjectUpdate(TxnHandle txn_old, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            try {
                TxnHandle txn = swift.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i = txn.get(id, false, swift.crdt.IntegerVersioned.class, this);
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
                        increment(swift);
                    } else {
                        swift.stop(true);
                    }
                } else {
                    txn.rollback();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class ObjectUpdatesListenerIncr2 extends AbstractObjectUpdatesListener {
        int expected;
        SwiftImpl swift;

        ObjectUpdatesListenerIncr2(int expected, SwiftImpl swift) {
            this.expected = expected;
            this.swift = swift;
        }

        @Override
        public void onObjectUpdate(TxnHandle txn_old, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            try {
                TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class, this);
                if (i1.getValue() == expected) {
                    i1.add(1);
                    handle.commit();
                    if (expected / 2 < iterations - 1) {
                        // wait for the system to settle down and finish
                        // internals
                        expected += 2;
                    } else {
                        swift.stop(true);
                    }
                } else {
                    handle.rollback();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}