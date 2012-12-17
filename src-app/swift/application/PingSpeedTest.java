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
package swift.application;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
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
                SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                        DCConstants.SURROGATE_PORT));
                runClient1(clientServer);
            }
        };
        client1.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread client2 = new Thread("client2") {
            public void run() {
                Sys.init();
                SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                        DCConstants.SURROGATE_PORT));
                runClient2(clientServer);
            }
        };
        client2.start();
    }

    static void runClient1(SwiftSession swift) {
        if (notifications) {
            client1CodeNotifications(swift);
        } else {
            client1Code(swift);
        }
    }

    static void runClient2(SwiftSession clientServer) {
        if (notifications) {
            client2CodeNotifications(clientServer);
        } else {
            client2Code(clientServer);
        }
    }

    protected static void client1Code(SwiftSession swift) {
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
                        // wait for the system to settle down
                        Thread.sleep(1000);
                        expected += 2;
                        timer.start();
                        increment(swift, null);
                    } else {
                        break;
                    }
                } else {
                    // System.out.println("Value " + i.getValue());
                    txn.rollback();
                }
            }
            swift.stopScout(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void client2Code(SwiftSession clientServer) {
        try {
            int expected = 1;
            while (true) {
                TxnHandle handle = clientServer.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class);
                if (i1.getValue() == expected) {
                    i1.add(1);
                    handle.commit();
                    if (expected / 2 < iterations - 1) {
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

    protected static void increment(SwiftSession swift, ObjectUpdatesListenerIncr1 listener) throws NetworkException,
            WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
        IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class, listener);
        i1.add(1);
        handle.commit();
    }

    protected static void client1CodeNotifications(SwiftSession swift) {
        try {
            System.out.println("Ping time");
            NanoTimeCollector timer = new NanoTimeCollector();

            int expected = 2;
            timer.start();
            TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
            IntegerTxnLocal i = handle.get(j, true, swift.crdt.IntegerVersioned.class, new ObjectUpdatesListenerIncr1(
                    expected, swift, timer));
            i.add(1);
            handle.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void client2CodeNotifications(SwiftSession clientServer) {
        try {
            int expected = 1;
            // Need cache policy MOST_RECENT for first read
            while (true) {
                TxnHandle handle = clientServer.beginTxn(isolationLevel, CachePolicy.MOST_RECENT, false);
                IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class,
                        new ObjectUpdatesListenerIncr2(expected + 2, clientServer));
                if (i1.getValue() == expected) {
                    i1.add(1);
                    handle.commit();
                    break;
                } else {
                    handle.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static class ObjectUpdatesListenerIncr1 extends AbstractObjectUpdatesListener {
        int expected;
        SwiftSession swift;
        NanoTimeCollector timer;

        ObjectUpdatesListenerIncr1(int expected, SwiftSession swift, NanoTimeCollector timer) {
            this.expected = expected;
            this.swift = swift;
            this.timer = timer;
        }

        @Override
        public void onObjectUpdate(TxnHandle txn_old, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            try {
                TxnHandle txn = swift.beginTxn(isolationLevel, cachePolicy, true);
                IntegerTxnLocal i = txn.get(id, true, swift.crdt.IntegerVersioned.class, this);
                DCConstants.DCLogger.info("PING 1 NOTIFICATION : " + i.getValue());
                if (expected == i.getValue()) {
                    long pingTime = timer.stop();
                    txn.commit();
                    System.out.println(pingTime);
                    if (expected / 2 < iterations) {
                        // wait for the system to settle down
                        Thread.sleep(1000);
                        expected += 2;
                        timer.start();
                        increment(swift, this);
                    } else {
                        swift.stopScout(true);
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
        SwiftSession swift;

        ObjectUpdatesListenerIncr2(int expected, SwiftSession clientServer) {
            this.expected = expected;
            this.swift = clientServer;
        }

        @Override
        public void onObjectUpdate(TxnHandle txn_old, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            try {
                TxnHandle handle = swift.beginTxn(isolationLevel, cachePolicy, false);
                IntegerTxnLocal i1 = handle.get(j, false, swift.crdt.IntegerVersioned.class, this);
                DCConstants.DCLogger.info("PING 2 NOTIFICATION : " + i1.getValue());
                if (i1.getValue() == expected) {
                    i1.add(1);
                    handle.commit();
                    if (expected / 2 < iterations - 1) {
                        expected += 2;
                    } else {
                        swift.stopScout(true);
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
