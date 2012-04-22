package sys.ring;

import static sys.Sys.Sys;

import java.util.Random;
import java.util.logging.Level;

import sys.dht.api.DHT;
import sys.dht.api.StringKey;
import sys.dht.catadupa.Catadupa;
import sys.dht.catadupa.Catadupa.Scope;
import sys.dht.test.KVS;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;
import sys.utils.Log;
import sys.utils.Threading;

public class Main {

    public static void main(String[] args) throws Exception {
        Log.setLevel("", Level.OFF);
        Log.setLevel("sys.dht.catadupa", Level.ALL);
        Log.setLevel("sys.net", Level.SEVERE);
        Log.setLevel("sys", Level.ALL);

        sys.Sys.init();

        Sys.setDatacenter( "datacenter-" + new Random().nextInt(3) );
        System.err.println( Sys.getDatacenter() );

        Catadupa.setScopeAndDomain(Scope.UNIVERSAL, "SwiftSequencerRing");
        new AbstractSequencerNode().init();

//        DHT stub = Sys.getDHT_ClientStub();
//
//        while (stub != null) {
//            String key = "" + Sys.rg.nextInt(1000);
//            stub.send(new StringKey(key), new StoreData(Sys.rg.nextDouble()), new KVS.ReplyHandler() {
//                @Override
//                public void onReceive(StoreDataReply reply) {
//                    System.out.println(reply.msg);
//                }
//            });
//            Threading.sleep(1000);
//        }

    }
}
