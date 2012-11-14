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
package sys.dht.test;

import static sys.Sys.Sys;

import java.util.Random;

import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.catadupa.Catadupa;
import sys.dht.catadupa.Catadupa.Scope;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;
import sys.utils.Threading;

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

		sys.Sys.init();

		// SeedDB.addSeedNode( Networking.resolve("10.0.0.1", 10001) ) ;

		Sys.setDatacenter("datacenter-" + new Random(1L).nextInt(1));

		Catadupa.setScopeAndDomain(Scope.DATACENTER, "SwiftDHT");
		DHT_Node.start();
		
		System.out.println("Ready...");
		
		DHT_Node.setHandler(new KVS.RequestHandler() {

			@Override
			public void onReceive(DHT.Handle conn, DHT.Key key, StoreData request) {
				System.out.printf("Got request for <%s, %s>\n", key, request.data);
				conn.reply(new StoreDataReply("OK " + request.data + "  " + Sys.getDatacenter()));
			}
		});

		Threading.sleep(1000);

		// int n = 0;
		// DHT stub = Sys.getDHT_ClientStub();
		//
		// System.out.println(stub.localEndpoint());
		//
		// while (stub != null) {
		// String key = "" + Sys.rg.nextInt(1000);
		// stub.send(new StringKey(key), new StoreData("" + n++), new
		// KVS.ReplyHandler() {
		// @Override
		// public void onReceive(StoreDataReply reply) {
		// System.out.println(reply.msg);
		// }
		// });
		// Threading.sleep(1000);
		// }
	}
}
