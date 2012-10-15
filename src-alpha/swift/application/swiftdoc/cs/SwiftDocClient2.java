package swift.application.swiftdoc.cs;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.List;

import swift.application.swiftdoc.SwiftDocOps;
import swift.application.swiftdoc.SwiftDocPatchReplay;
import swift.application.swiftdoc.TextLine;
import swift.application.swiftdoc.cs.msgs.AckHandler;
import swift.application.swiftdoc.cs.msgs.AppRpcHandler;
import swift.application.swiftdoc.cs.msgs.BeginTransaction;
import swift.application.swiftdoc.cs.msgs.BulkTransaction;
import swift.application.swiftdoc.cs.msgs.CommitTransaction;
import swift.application.swiftdoc.cs.msgs.InitScoutServer;
import swift.application.swiftdoc.cs.msgs.InsertAtom;
import swift.application.swiftdoc.cs.msgs.RemoveAtom;
import swift.application.swiftdoc.cs.msgs.ServerReply;
import swift.application.swiftdoc.cs.msgs.SwiftDocRpc;
import swift.dc.DCConstants;
import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.impl.rpc.RpcEcho;
import sys.net.impl.rpc.RpcEchoHandler;
import sys.net.impl.rpc.RpcPing;
import sys.net.impl.rpc.RpcPong;
import sys.scheduler.PeriodicTask;
import sys.utils.Threading;
import umontreal.iro.lecuyer.stat.Tally;

/**
 * 
 * @author smduarte
 * 
 */
public class SwiftDocClient2 {

    public static void main(String[] args) {
        System.out.println("SwiftDoc Client start!");

        if (args.length == 0)
            args = new String[] { "localhost", "1" };

        sys.Sys.init();

        final String server = args[0];

        Threading.newThread("client1", true, new Runnable() {
            public void run() {
                runClient1Code(server, server);
            }
        }).start();

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                runClient2Code(server);
            }
        }).start();
    }

    static void runClient1Code(String server, String dcName) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT1);
        Endpoint dc = Networking.resolve(dcName, DCConstants.SURROGATE_PORT);
        client1Code(srv, dc);
    }

    static void runClient2Code(String server) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT2);
        client2Code(srv);
    }

    /*
     * Replay the document patching operations. Each patch will be cause an rpc
     * call for each update operation performed on the CRDT. -beginTransaction
     * -insertAt -removeAt -commitTransaction
     * 
     * read operations are performed locally on a mirror version of the
     * document...
     */

    static TransportConnection pingCon = null;

    static void client1Code(final Endpoint server, final Endpoint DC) {
        final Tally dcRTT = new Tally();

        // Initiate measurement of RTT to central datacenter...

        final Endpoint pinger = Networking.bind(0, new RttHandler() {
            @Override
            public void onReceive(TransportConnection conn, RpcPong pong) {
                dcRTT.add(pong.rtt());
            }
        });
        
        new PeriodicTask(0.0, 1.0) {
            public void run() {
                if (pingCon == null)
                    pingCon = pinger.send(DC, new RpcEcho());
                else {
                    if (!pingCon.send(new RpcPing()))
                        pingCon = null;
                }
            }
        };

        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        final List<Long> results = new ArrayList<Long>();

        SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

        endpoint.send(server, new InitScoutServer(), new AppRpcHandler() {
            public void onReceive(final ServerReply r) {
                synchronized (results) {
                    for (TextLine i : r.atoms)
                        if (!i.isWarmUp())
                            results.add(i.latency());
                }
                System.err.println("Got: " + r.atoms.size() + "/" + results.size());
            }

        });

        final int BATCHSIZE = 10;
        try {
            // Warmup Phase...
//           player.parseFiles( new SwiftDocOpsImpl(true, BATCHSIZE, endpoint, server, 10));
//
//            // For real now...
            player.parseFiles(new SwiftDocOpsImpl(false, BATCHSIZE, endpoint, server, 250));

        } catch (Exception x) {
            x.printStackTrace();
        }

        double t, t0 = System.currentTimeMillis() + 30000;
        while ((t = System.currentTimeMillis()) < t0) {
            System.err.printf("\rWaiting: %s", (t0 - t) / 1000);
            Threading.sleep(10);
        }

        synchronized (results) {
            System.out.printf("# RTT to %s min: %s max: %s avg: %s std: %s\n", DC, dcRTT.min(), dcRTT.max(),
                    dcRTT.average(), dcRTT.standardDeviation());
            for (Long i : results)
                System.out.printf("%s\n", i );
        }
        System.exit(0);
    }

    // ------------------------------------------------------------------------
    static class SwiftDocOpsImpl implements SwiftDocOps<TextLine> {
        List<TextLine> mirror = new ArrayList<TextLine>();

        int batchsize;
        boolean warmup;
        RpcEndpoint endpoint;
        Endpoint server;
        int delay;
        final AckHandler ackHandler = new AckHandler();
        
        final List<SwiftDocRpc> ops = new ArrayList<SwiftDocRpc>();

        SwiftDocOpsImpl(boolean warmup, int batchSize, RpcEndpoint endpoint, Endpoint server, int delay) {
            this.delay = delay;
            this.warmup = warmup;
            this.server = server;
            this.endpoint = endpoint;
            this.batchsize = batchSize;
        }

        public void begin() {
        }

        @Override
        public void add(int pos, TextLine atom) {
            ops.add(new InsertAtom(atom, pos));
            if (ops.size() >= batchsize)
                commit();
            mirror.add(pos, atom);
        }

        public TextLine remove(int pos) {
            ops.add(new RemoveAtom(pos));
            if (ops.size() >= batchsize)
                commit();
            return mirror.remove(pos);
        }

        @Override
        public void commit() {
            endpoint.send(server, new BulkTransaction(ops), ackHandler);
            Threading.sleep(delay);
            ops.clear();
        }

        @Override
        public TextLine get(int pos) {
            return mirror.get(pos);
        }

        @Override
        public int size() {
            return mirror.size();
        }

        @Override
        public TextLine gen(String s) {
            return new TextLine(s, warmup);
        }
    }

    // ------------------------------------------------------------------------

    // Echo the atoms received to the server...
    static void client2Code(final Endpoint server) {

        final AckHandler ackHandler = new AckHandler();

        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        try {
            endpoint.send(server, new InitScoutServer(), new AppRpcHandler() {
                synchronized public void onReceive(final ServerReply r) {
                                endpoint.send( server, new BeginTransaction(), ackHandler, 0 ) ;
                                for( TextLine i : r.atoms )
                                    endpoint.send( server, new InsertAtom(i , -1), ackHandler, 0 );
                                
                                endpoint.send( server, new CommitTransaction(), ackHandler);
//                                BulkTransaction t = new BulkTransaction(new ArrayList<SwiftDocRpc>());
//                                for (TextLine i : r.atoms)
//                                    t.ops.add(  new InsertAtom(i, -1) ) ;                                
//                                endpoint.send(server, t , ackHandler);
                }
            });

        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    static class RttHandler implements RpcEchoHandler {

        @Override
        public void onAccept(TransportConnection conn) {
        }

        @Override
        public void onConnect(TransportConnection conn) {
        }

        @Override
        public void onFailure(TransportConnection conn) {
        }

        @Override
        public void onClose(TransportConnection conn) {
        }

        @Override
        public void onFailure(Endpoint dst, Message m) {
        }

        @Override
        public void onReceive(TransportConnection conn, Message m) {
        }

        @Override
        public void onReceive(TransportConnection conn, RpcEcho echo) {
        }

        @Override
        public void onReceive(TransportConnection conn, RpcPing ping) {
        }

        @Override
        public void onReceive(TransportConnection conn, RpcPong pong) {
        }
    }
}
