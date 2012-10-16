package swift.dc;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import sys.Sys;

public class TestIncClient {
    public static void main(String[] args) {
        try {
            if( args.length != 3) {
                System.out.println( "Use: jaba swift.dc.TestIncClient surrogate_node table key");
                return;
            }
            String serverNode = args[0];
            String table = args[1];
            String key = args[2];
            
            Sys.init();

            Swift server = SwiftImpl.newInstance(new SwiftOptions(serverNode, DCConstants.SURROGATE_PORT));
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier(table, key), false, swift.crdt.IntegerVersioned.class);
            System.out.println("(" + table + "," + key + ") = " + i1.getValue());
            i1.add(1);
            System.out.println("(" + table + "," + key + ") = " + i1.getValue());
            handle.commit();
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
