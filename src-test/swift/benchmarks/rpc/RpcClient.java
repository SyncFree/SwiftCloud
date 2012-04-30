package swift.benchmarks.rpc;

import static sys.net.api.Networking.Networking;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.utils.Threading;

import static sys.Sys.*;

public class RpcClient {

	public static void main(String[] args) {

		sys.Sys.init();

		KryoSerialization.init();
		
		RpcEndpoint endpoint = Networking.rpcBind(0, null);
		final Endpoint server = Networking.resolve("localhost", RpcServer.PORT);

		int total = 0;
		double T0 = Sys.currentTime();
		
		int n = 0;
		final Set<Integer> values = Collections.synchronizedSet(new HashSet<Integer>()) ;
		
		for (;;) {
		    values.add( n ) ;
			endpoint.send(server, new Request( n++ ), new Handler() {

				@Override
				public void onFailure() {
					System.out.println("Send failed...");
				}

				@Override
				public void onReceive(Reply r) {
				    values.remove( r.val ) ;
//					System.out.println("Got: " + r + " from:" + conn.remoteEndpoint() + ":" + Thread.currentThread());
				}

			});
			total++;
			if( total % 1000 == 999 ) {
			    System.out.println(total + " RPCs/sec:" + total / (Sys.currentTime() - T0) + "   " + values.size());
			}
		}

	}
}
