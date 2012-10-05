package sys.dht.test;

import static sys.Sys.Sys;

import sys.dht.api.DHT;
import sys.dht.api.StringKey;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;
import sys.utils.Threading;

/**
 * 
 * An example of a client application interacting with the DHT.
 * 
 * Sends a request to the DHT (Key + Data) and (asynchronously) awaits a reply.
 * 
 * Note that to simplify binding, multicast is currently used to discover the
 * endpoint of the DHT node.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public class Client {

	public static void main(String[] args) throws Exception {
		sys.Sys.init();
		
		Sys.setDatacenter("datacenter-0");

		DHT stub = Sys.getDHT_ClientStub();

		while (stub != null) {
			String key = "" + Sys.rg.nextInt(1000);
			stub.send(new StringKey(key), new StoreData(Sys.rg.nextDouble()), new KVS.ReplyHandler() {
			    public void onFailure() {
			        System.out.println("Failed...");
			    }
				public void onReceive(StoreDataReply reply) {
					System.out.println(reply.msg);
				}
			});
			Threading.sleep(1000);
		}
		
//		while (stub != null) {
//			String key = "" + Sys.rg.nextInt(1000);
//			Endpoint res = stub.resolveKey(new StringKey(key), 1000);
//			System.out.println( "Resolved DHT key:" + key + " to Node: " + res );
//			Threading.sleep(1000);
//		}
	}
}
