package sys.dht.test;

import java.util.Random;
import java.util.logging.Level;

import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.StringKey;
import sys.dht.catadupa.Catadupa;
import sys.dht.catadupa.Catadupa.Scope;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;
import sys.utils.Log;
import sys.utils.Threading;

import static sys.Sys.*;

 /* 
 * An example of the server-side of the DHT. As such, this will instantiate a
 * DHT node.
 * 
 * In this example, DHT nodes await requests of (Key + Message(StoreData)).
 * 
 * Any DHT node, upon receiving a request, will forward it, in 1-hop, to the DHT
 * node responsible for the request's key (ie., the sucessor node of the hash of
 * the key). If the client expected a reply, the last DHT node will reply
 * directly, by establishing a new connection to the client node.
 * 
 * Note that to simplify binding, multicast is currently used to discover the
 * endpoint of a seed DHT node.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public class Server {

	public static void main(String[] args) throws Exception {
		Log.setLevel("", Level.OFF);
		Log.setLevel("sys.dht.catadupa", Level.FINER);
		Log.setLevel("sys.dht", Level.FINEST);
		Log.setLevel("sys.net", Level.SEVERE);
		Log.setLevel("sys", Level.OFF);

		sys.Sys.init();
		Sys.setDatacenter( "datacenter-" + new Random().nextInt(3) );		
		System.err.println( Sys.getDatacenter() );
		
		Catadupa.setScopeAndDomain( Scope.DATACENTER, "SwiftDHT") ;
		DHT_Node.start();

		DHT_Node.setHandler(new KVS.RequestHandler() {
			@Override
			public void onReceive(DHT.Connection conn, DHT.Key key, StoreData request) {
				System.out.printf("Got request for <%s, %s>\n", key, request.data);
				conn.reply(new StoreDataReply("OK " + request.data + "  " + Sys.getDatacenter() ));
			}
		});

		Threading.sleep(1000);

//		int n = 0;
//		DHT stub = Sys.getDHT_ClientStub();
//		while (stub != null) {
//			String key = "" + Sys.rg.nextInt(1000);
//			stub.send(new StringKey(key), new StoreData("" + n++), new KVS.ReplyHandler() {
//				@Override
//				public void onReceive(StoreDataReply reply) {
//					System.out.println(reply.msg);
//				}
//			});
//			Threading.sleep(1000);
//		}
	}
}
