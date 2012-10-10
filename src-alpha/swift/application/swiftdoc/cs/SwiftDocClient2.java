package swift.application.swiftdoc.cs;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.List;

import sun.rmi.transport.tcp.TCPConnection;
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
import swift.application.swiftdoc.cs.msgs.ServerACK;
import swift.application.swiftdoc.cs.msgs.ServerReply;
import swift.application.swiftdoc.cs.msgs.SwiftDocRpc;
import swift.dc.DCConstants;
import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.impl.DefaultMessageHandler;
import sys.net.impl.providers.TcpPing;
import sys.net.impl.providers.TcpPong;
import sys.net.impl.rpc.RpcPacket;
import sys.net.impl.rpc.RpcPing;
import sys.net.impl.rpc.RpcPingPongHandler;
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

        if( args.length == 0)
            args = new String[] { "localhost", "1" };

        sys.Sys.init();

        final String server = args[0];
        
        Threading.newThread("client1", true, new Runnable() {
            public void run() {
                runClient1Code( server, server );
            }
        }).start();

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                runClient2Code( server );
            }
        }).start();
    }


    static void runClient1Code( String server, String dcName ) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT1);
        Endpoint dc = Networking.resolve(dcName, DCConstants.SURROGATE_PORT);
        client1Code( srv, dc );
    }

    static void runClient2Code( String server ) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT2);
        client2Code( srv );
    }

    static final int timeout = SwiftDocServer.synchronousOps ? 5000 : 0;

    
    /*
     * Replay the document patching operations. 
     * Each patch will be cause an rpc call for each update operation performed on the CRDT.
     * -beginTransaction
     * -insertAt
     * -removeAt
     * -commitTransaction
     * 
     * read operations are performed locally on a mirror version of the document...
     */
    
    static TransportConnection pingCon = null;
    static void client1Code( final Endpoint server, final Endpoint DC ) {
        final Tally dcRTT = new Tally();

        //Initiate measurement of RTT to central datacenter... 
        
        final Endpoint pinger = Networking.bind(0, new RttHandler() {
            @Override
            public void onReceive(TransportConnection conn, RpcPong pong) {
                dcRTT.add( pong.rtt() ) ;
            }
        });
        new PeriodicTask(0, 2.5) {
            public void run() {
         
                if( pingCon == null )
                    pingCon = pinger.send( DC, new RpcPing() ) ;
                else {
                    if( ! pingCon.send( new RpcPing() ) ) 
                        pingCon = null;
                }
                System.err.println( dcRTT.report() );
            }
        };
        
        if( true )
            return;
        
        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        final List<Long> results = new ArrayList<Long>();

        SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

        endpoint.send( server, new InitScoutServer(), new AppRpcHandler() {
            public void onReceive(final ServerReply r) {
                synchronized( results ) {
                    for( TextLine i : r.atoms ) {
                        results.add( i.latency() ) ;
                    }                            
                }
                System.err.println( "Got: " + r.atoms.size() + "/" + results.size() );
            }  
            
        } );
        
        final AckHandler ackHandler = new AckHandler();

        final List<SwiftDocRpc> ops = new ArrayList<SwiftDocRpc>();
        final int BATCHSIZE = 10;
        try {
            player.parseFiles(new SwiftDocOps<TextLine>() {
                List<TextLine> mirror = new ArrayList<TextLine>();

                @Override
                public void begin() {
                }

                @Override
                public void add(int pos, TextLine atom) {
                    ops.add( new InsertAtom(atom, pos) ) ;
                    if( ops.size() >= BATCHSIZE )
                        commit();
                    mirror.add(pos, atom);
                }

                public TextLine remove(int pos) {
                    ops.add( new RemoveAtom(pos) ) ;
                    if( ops.size() >= BATCHSIZE )
                        commit();
                    return mirror.remove(pos);
                }

                @Override
                public void commit() {
                    endpoint.send( server, new BulkTransaction( ops ), ackHandler ) ;
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
                    return new TextLine(s);
                }
            }, 50);
        } catch (Exception x) {
            x.printStackTrace();
        }

        double t, t0 = System.currentTimeMillis() + 30000;
        while( (t = System.currentTimeMillis()) < t0 ) {
                System.err.printf("\rWaiting: %s", (t0 - t)/1000);
                Threading.sleep(10);
        }
        
        synchronized( results ) {
            for (Long i : results )
                System.out.printf("%.1f\n", 100.0 * i / dcRTT.average() );
        }
        System.exit(0);
    }

    //Echo the atoms received to the server...
    static void client2Code( final Endpoint server ) {
        final Object barrier = new Object();

        final AckHandler ackHandler = new AckHandler();
        
        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        try {
            endpoint.send(server, new InitScoutServer(), new AppRpcHandler() {
                public void onReceive(final ServerReply r) {

                    Threading.newThread(true, new Runnable() {
                        public void run() {
                            synchronized (barrier) {
                                endpoint.send(server, new BeginTransaction(), ackHandler, timeout);
                                for (TextLine i : r.atoms)
                                    endpoint.send(server, new InsertAtom(i, -1), ackHandler, timeout);

                                endpoint.send(server, new CommitTransaction(), ackHandler);
                            }
                        }
                    }).start();
                }
            }) ;

        } catch (Exception x) {
            x.printStackTrace();
        }
    }

static class RttHandler implements RpcPingPongHandler {

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
    public void onReceive(TransportConnection conn, RpcPing ping) {
    }

    @Override
    public void onReceive(TransportConnection conn, RpcPong pong) {
    }    
}
}
