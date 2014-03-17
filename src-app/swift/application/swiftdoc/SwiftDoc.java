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
package swift.application.swiftdoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.SequenceCRDT;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;
import sys.utils.Threading;

/**
 * 
 * @author smduarte, annettebieniusa
 * 
 */
public class SwiftDoc {
    private static String sequencerName = "localhost";
    private static String dcName = "localhost";
    static int iterations = 10;
    static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;
    static CachePolicy cachePolicy = CachePolicy.CACHED;
    static boolean notifications = true;
    public static CRDTIdentifier j1 = new CRDTIdentifier("doc", "1");
    public static CRDTIdentifier j2 = new CRDTIdentifier("doc", "2");

    public static void main(String[] args) {
        System.out.println("SwiftDoc start!");
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        // start DC server
        DCServer.main(new String[] { dcName });

        final SwiftOptions options = new SwiftOptions("localhost", DCConstants.SURROGATE_PORT);
        options.setDisasterSafe(false);
        options.setConcurrentOpenTransactions(true);

        final SwiftSession swift1 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                DCConstants.SURROGATE_PORT));
        final SwiftSession swift2 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName,
                DCConstants.SURROGATE_PORT));

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                Sys.init();
                runClient1(swift1);
            }
        }).start();

        Threading.sleep(1000);

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                Sys.init();
                runClient2(swift2);
            }
        }).start();
    }

    static void runClient1(SwiftSession swift1) {
        client1code(swift1);
    }

    static void runClient2(SwiftSession swift2) {
        client2code(swift2);
    }

    static void client1code(final SwiftSession swift1) {
        try {
            final AtomicBoolean done = new AtomicBoolean(false);
            final Map<Long, TextLine> samples = new HashMap<Long, TextLine>();
            final Semaphore semaphore = new Semaphore(0);

            Threading.newThread(true, new Runnable() {
                public void run() {
                    try {
                        while (!done.get()) {
                            final TxnHandle handle = swift1.beginTxn(isolationLevel, CachePolicy.CACHED, true);

                            SequenceCRDT<TextLine> doc = getDoc(handle, j2, false, new AbstractObjectUpdatesListener() {
                                public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, CRDT<?> previousValue) {
                                    System.err.printf("CLIENT 1");
                                    semaphore.release();
                                }
                            });
                            handle.commit();

                            for (TextLine i : doc.getValue()) {
                                if (!samples.containsKey(i.serial())) {
                                    samples.put(i.serial(), i);
                                }
                            }
                            semaphore.acquireUninterruptibly();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

            player.parseFiles(new SwiftDocOps<TextLine>() {
                TxnHandle handle = null;
                SequenceCRDT<TextLine> doc = null;
                int cm = 0;

                @Override
                public void begin() {
                    try {
                        handle = swift1.beginTxn(isolationLevel, cachePolicy, false);
                        doc = getDoc(handle, j1, true, null);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.exit(0);
                    }
                }

                public TextLine remove(int pos) {
                    return doc.removeAt(pos);
                }

                @Override
                public TextLine get(int pos) {
                    return doc.getValue().get(pos);
                }

                @Override
                public void add(int pos, TextLine atom) {
                    doc.insertAt(pos, atom);
                }

                @Override
                public int size() {
                    return doc.size();
                }

                @Override
                public void commit() {
                    cm++;
                    handle.commit();
                }

                @Override
                public TextLine gen(String s) {
                    return new TextLine(s, cm);
                }
            });
            done.set(true);
            Threading.sleep(5000);

            for (TextLine i : new ArrayList<TextLine>(samples.values()))
                System.out.printf("%s\t%s\n", i.latency(), i.commit);

            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static ExecutorService executor = Executors.newFixedThreadPool(1);

    static void client2code(final SwiftSession swift2) {
        try {
            final Set<Long> serials = new HashSet<Long>();
            final Semaphore barrier = new Semaphore(0);

            for (;;) {
                final TxnHandle handle = swift2.beginTxn(isolationLevel, CachePolicy.CACHED, true);
                SequenceCRDT<TextLine> doc = getDoc(handle, j1, false, new AbstractObjectUpdatesListener() {
                    public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, CRDT<?> previousValue) {
                        // System.err.printf("CLIENT 2: %s\n", j1);
                        barrier.release();
                    }
                });

                final SequenceCRDT<TextLine> doc2 = doc;
                executor.execute(new Runnable() {
                    public void run() {
                        try {
                            Collection<TextLine> newAtoms = new ArrayList<TextLine>();
                            synchronized (serials) {
                                for (TextLine i : doc2.getValue()) {
                                    if (serials.add(i.serial())) {
                                        newAtoms.add(i);
                                    }
                                }
                            }
                            System.err.println("------->" + newAtoms.size());
                            TxnHandle handle = swift2.beginTxn(isolationLevel, CachePolicy.CACHED, false);
                            SequenceCRDT<TextLine> doc3 = getDoc(handle, j2, true, null);
                            for (TextLine i : newAtoms)
                                doc3.insertAt(doc3.size(), i);
                            handle.commit();
                        } catch (Exception x) {
                            x.printStackTrace();
                        }
                    }
                });
                handle.commit();

                // Wait for the notification, before reading again the new value
                // of
                // the sequence...
                barrier.acquireUninterruptibly();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("unchecked")
    static SequenceCRDT<TextLine> getDoc(TxnHandle handle, CRDTIdentifier id, boolean create,
            AbstractObjectUpdatesListener listener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {

        for (;;) {
            try {
                return (SequenceCRDT<TextLine>) handle.get(id, create, swift.crdt.SequenceCRDT.class, listener);
            } catch (Exception x) {
                Threading.sleep(10);
            }
        }
    }
}
