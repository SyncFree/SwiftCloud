package swift.application.swiftdoc.cs;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import swift.application.swiftdoc.SwiftDocOps;
import swift.application.swiftdoc.SwiftDocPatchReplay;
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
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.scheduler.PeriodicTask;
import sys.utils.Threading;

/**
 * 
 * @author smduarte
 * 
 */
public class SwiftDocClient {

    public static void main(String[] args) {
        System.out.println("SwiftDoc Client start!");

        if( args.length == 0)
            args = new String[] { "localhost", "1" };

        sys.Sys.init();

        final String server = args[0];
        
        Threading.newThread("client1", true, new Runnable() {
            public void run() {
                runClient1Code( server );
            }
        }).start();

        Threading.newThread("client2", true, new Runnable() {
            public void run() {
                runClient2Code( server );
            }
        }).start();
    }


    static void runClient1Code( String server ) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT1);
        client1Code( srv );
    }

    static void runClient2Code( String server ) {
        Endpoint srv = Networking.resolve(server, SwiftDocServer.PORT2);
        client2Code( srv );
    }

    
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
    static void client1Code( final Endpoint server ) {
        final int TIMEOUT = Integer.MAX_VALUE >> 1;
        
        final RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        final List<Long> results = new ArrayList<Long>();

        SwiftDocPatchReplay<TextLine> player = new SwiftDocPatchReplay<TextLine>();

        endpoint.send( server, new InitScoutServer(), new AppRpcHandler() {
            public void onReceive(final ServerReply r) {
                for( TextLine i : r.atoms )
                    results.add( i.latency() ) ; 
                System.err.println( "Got: " + r.atoms.size() + "/" + results.size() );
            }  
            
        } );
        
        final AckHandler ackHandler = new AckHandler();
        
        try {
            player.parseFiles(new SwiftDocOps<TextLine>() {
                List<TextLine> mirror = new ArrayList<TextLine>();

                @Override
                public void begin() {
                    endpoint.send( server, new BeginTransaction(), ackHandler ) ;
                }

                @Override
                public void add(int pos, TextLine atom) {
                    endpoint.send( server, new InsertAtom(atom, pos), ackHandler ) ;
                    mirror.add(pos, atom);
                }

                public TextLine remove(int pos) {
                    endpoint.send( server, new RemoveAtom(pos), ackHandler ) ;
                    return mirror.remove(pos);
                }

                @Override
                public void commit() {
                    endpoint.send( server, new CommitTransaction(), ackHandler ) ;
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
            }, 1);
        } catch (Exception x) {
            x.printStackTrace();
        }

        for (Long i : results )
            System.out.printf("%s\n", i);
        
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
                                endpoint.send(server, new BeginTransaction(), ackHandler );
                                for (TextLine i : r.atoms)
                                    endpoint.send(server, new InsertAtom(i, -1), ackHandler);

                                endpoint.send(server, new CommitTransaction(), ackHandler );
                            }
                        }
                    }).start();
                }
            }) ;

        } catch (Exception x) {
            x.printStackTrace();
        }
    }
}

class AckHandler extends AppRpcHandler {
    public void onReceive(final ServerACK r) {
    }
}