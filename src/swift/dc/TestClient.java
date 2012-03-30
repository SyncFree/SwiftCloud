package swift.dc;

import static sys.net.api.Networking.Networking;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcEndpoint;
import swift.client.*;
import swift.crdt.CRDTIdentifier;
import swift.crdt.*;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.TxnHandle;

public class TestClient {
    public static void main( String []args) {
        try {
        Sys.init();

        RpcEndpoint localEP = Networking.rpcBind(0, null);
        final Endpoint serverEP = Networking.resolve("localhost", DCConstants.SURROGATE_PORT);

        SwiftImpl server = new SwiftImpl( localEP, serverEP);
        TxnHandle handle = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, false);
        IntegerTxnLocal i1 = handle.get( new CRDTIdentifier( "e", "1"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(e,1) = " + i1.getValue());
        IntegerTxnLocal i2 = handle.get( new CRDTIdentifier( "e", "2"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(e,2) = " + i2.getValue());
        i1.add(1);
        System.out.println( "(e,1).add(1)");
        System.out.println( "(e,1) = " + i1.getValue());
        handle.commit();
        System.out.println( "commit");
        
        handle = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, false);
        i1 = handle.get( new CRDTIdentifier( "e", "1"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(e,1) = " + i1.getValue());
        i2 = handle.get( new CRDTIdentifier( "e", "2"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(e,2) = " + i2.getValue());
        i1.add(1);
        System.out.println( "(e,1).add(1)");
        System.out.println( "(e,1) = " + i1.getValue());
        handle.commit();
        System.out.println( "commit");
        
        handle = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, false);
        i1 = handle.get( new CRDTIdentifier( "t", "1"), true, swift.crdt.IntegerVersioned.class);
        System.out.println( "(t,1) = " + i1.getValue());
        i2 = handle.get( new CRDTIdentifier( "t", "2"), true, swift.crdt.IntegerVersioned.class);
        System.out.println( "(t,2) = " + i2.getValue());
        i1.add(1);
        System.out.println( "(t,1).add(1)");
        System.out.println( "(t,1) = " + i1.getValue());
        handle.commit();
        System.out.println( "commit");

        handle = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, false);
        i1 = handle.get( new CRDTIdentifier( "t", "1"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(t,1) = " + i1.getValue());
        i2 = handle.get( new CRDTIdentifier( "t", "2"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(t,2) = " + i2.getValue());
        i1.add(1);
        System.out.println( "(t,1).add(1)");
        System.out.println( "(t,1) = " + i1.getValue());
        handle.commit();
        System.out.println( "commit");
        
        handle = server.beginTxn(CachePolicy.STRICTLY_MOST_RECENT, false);
        i1 = handle.get( new CRDTIdentifier( "t", "1"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(t,1) = " + i1.getValue());
        i2 = handle.get( new CRDTIdentifier( "t", "2"), false, swift.crdt.IntegerVersioned.class);
        System.out.println( "(t,2) = " + i2.getValue());
        i1.add(1);
        System.out.println( "(t,1).add(1)");
        System.out.println( "(t,1) = " + i1.getValue());
        handle.commit();
        System.out.println( "commit");
        
        System.out.println( "TetsClient ended with success");
        } catch( Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
