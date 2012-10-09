package swift.application.swiftdoc.cs;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.application.swiftdoc.TextLine;
import swift.application.swiftdoc.cs.msgs.AppRpcHandler;
import swift.application.swiftdoc.cs.msgs.BeginTransaction;
import swift.application.swiftdoc.cs.msgs.CommitTransaction;
import swift.application.swiftdoc.cs.msgs.InitScoutServer;
import swift.application.swiftdoc.cs.msgs.InsertAtom;
import swift.application.swiftdoc.cs.msgs.RemoveAtom;
import swift.application.swiftdoc.cs.msgs.ServerACK;
import swift.application.swiftdoc.cs.msgs.ServerReply;
import swift.client.AbstractObjectUpdatesListener;
import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.SequenceTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.PeriodicTask;
import sys.utils.IP;
import sys.utils.Threading;
import umontreal.iro.lecuyer.stat.Tally;

/**
 * 
 * @author smduarte
 * 
 */
public class SwiftDocServer extends Thread {
    public static int PORT1 = 11111, PORT2 = 11112;

    static String dcName = "localhost";
    private static String sequencerName = "localhost";

    static boolean synchronousOps = false;

    static boolean notifications = true;
    static long cacheEvictionTimeMillis = 60000;
    static CachePolicy cachePolicy = CachePolicy.CACHED;
    static IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READS;

    static CRDTIdentifier j1id = new CRDTIdentifier("swiftdoc1", "1");
    static CRDTIdentifier j2id = new CRDTIdentifier("swiftdoc2", "1");

    public static void main(String[] args) {
        System.out.println("SwiftDoc Server start!");
        Sys.init();

        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        // start DC server
        DCServer.main(new String[] { dcName });

        Threading.sleep(5000);
        System.out.println("SwiftDoc Launching scouts...!");

        Threading.newThread("scoutServer1", true, new Runnable() {
            public void run() {
                runScoutServer1();
            }
        }).start();

        Threading.newThread("scoutServer2", true, new Runnable() {
            public void run() {
                runScoutServer2();
            }
        }).start();
    }

    static void runScoutServer1() {
        scoutServerCommonCode(PORT1, j1id, j2id);
    }

    static void runScoutServer2() {
        scoutServerCommonCode(PORT2, j2id, j1id);
    }

    public void run() {
        notifyClient();
    }

    static void scoutServerCommonCode(final int port, final CRDTIdentifier d1, final CRDTIdentifier d2) {
        try {

            Networking.rpcBind(port, TransportProvider.DEFAULT).toService(0, new AppRpcHandler() {

                public void onReceive(RpcHandle client, final InitScoutServer r) {
                    client.enableDeferredReplies(Integer.MAX_VALUE);
                    getSession(client.remoteEndpoint(), client, d1, d2);
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final BeginTransaction r) {
                    getSession(client.remoteEndpoint()).swiftdoc.begin();
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final CommitTransaction r) {
                    getSession(client.remoteEndpoint()).swiftdoc.commit();
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final InsertAtom r) {
                    getSession(client.remoteEndpoint()).swiftdoc.add(r.pos, r.atom);
                    client.reply(new ServerACK(r));
                }

                public void onReceive(final RpcHandle client, final RemoveAtom r) {
                    getSession(client.remoteEndpoint()).swiftdoc.remove(r.pos);
                    client.reply(new ServerACK(r));
                }
            });
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    TxnHandle handle = null;
    CRDTIdentifier j1 = null, j2 = null;
    SequenceTxnLocal<TextLine> doc = null;

    RpcHandle clientHandle = null;
    SwiftImpl swift1 = null, swift2 = null;

    SwiftDocServer(SwiftImpl swift1, SwiftImpl swift2, RpcHandle client, CRDTIdentifier j1, CRDTIdentifier j2) {
        this.j1 = j1;
        this.j2 = j2;
        this.swift1 = swift1;
        this.swift2 = swift2;
        this.clientHandle = client.enableDeferredReplies(Integer.MAX_VALUE);
    }

    public void begin() {
        try {
            handle = swift1.beginTxn(isolationLevel, cachePolicy, false);
            doc = handle.get(j1, true, swift.crdt.SequenceVersioned.class, null);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void add(int pos, TextLine atom) {
        if (pos < 0)
            pos = doc.size();

        doc.insertAt(pos, atom);
    }

    public TextLine remove(int pos) {
        return doc.removeAt(pos);
    }

    public void commit() {
        handle.commit();
        handle = null;
        doc = null;
    }

    void notifyClient() {
        try {
            final Set<Long> serials = new HashSet<Long>();
            for (int k = 0; true; k++) {

                final Object barrier = new Object();
                final TxnHandle handle = swift2.beginTxn(isolationLevel, k == 0 ? CachePolicy.MOST_RECENT
                        : CachePolicy.CACHED, true);

                SequenceTxnLocal<TextLine> doc = handle.get(j2, true, swift.crdt.SequenceVersioned.class,
                        new AbstractObjectUpdatesListener() {
                            public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {

                                Threading.synchronizedNotifyAllOn(barrier);
                                // System.err.println("Triggered Reader get():"
                                // + j2 + "  "+ previousValue.getValue());
                            }
                        });

                List<TextLine> newAtoms = new ArrayList<TextLine>();
                for (TextLine i : doc.getValue())
                    if (serials.add(i.serial())) {
                        newAtoms.add(i);
                    }
                handle.commit();

                if (newAtoms.size() > 0)
                    clientHandle.reply(new ServerReply(newAtoms));

                Threading.synchronizedWaitOn(barrier, k == 0 ? 1 : 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Session getSession(Object sessionId) {
        return sessions.get(sessionId);
    }

    static Session getSession(Object sessionId, RpcHandle client, CRDTIdentifier j1, CRDTIdentifier j2) {
        Session res = sessions.get(sessionId);
        if (res == null) {
            sessions.put(sessionId, res = new Session(client, j1, j2));
        }
        return res;
    }

    static Map<Object, Session> sessions = new HashMap<Object, Session>();

    static class Session {
        final SwiftImpl swift1, swift2;
        final RpcHandle client;
        final SwiftDocServer swiftdoc;

        Session(RpcHandle client, CRDTIdentifier j1, CRDTIdentifier j2) {
            this.client = client;

            this.swift1 = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT, false,
                    SwiftImpl.DEFAULT_TIMEOUT_MILLIS, Integer.MAX_VALUE, cacheEvictionTimeMillis);

            this.swift2 = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT, false,
                    SwiftImpl.DEFAULT_TIMEOUT_MILLIS, Integer.MAX_VALUE, cacheEvictionTimeMillis);

            swiftdoc = new SwiftDocServer(swift1, swift2, client, j1, j2);
            swiftdoc.start();
        }
    }

}
