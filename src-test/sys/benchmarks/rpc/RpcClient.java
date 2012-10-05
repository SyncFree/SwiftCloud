package sys.benchmarks.rpc;

import static sys.net.api.Networking.Networking;

import java.net.UnknownHostException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

import static sys.Sys.*;

public class RpcClient {
    public static Logger Log = Logger.getLogger( RpcClient.class.getName() );
    
	static double sumRTT = 0, totRTT = 0;

	public void doIt(String serverAddr) {

		RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

		final Endpoint server = Networking.resolve(serverAddr, RpcServer.PORT);

		double T0 = Sys.currentTime();

		final SortedSet<Integer> values = new TreeSet<Integer>();

		for (int n = 0;; n++) {
			synchronized (values) {
				values.add(n);
			}

			endpoint.send(server, new Request(n), new Handler() {

				@Override
				public void onFailure(RpcHandle handle) {
					System.out.println("Send failed...");
				}

				@Override
				public void onReceive(Reply r) {
					synchronized (values) {
						values.remove(r.val);
					}
					sumRTT += r.rtt();
					totRTT++;
				}

			}).getReply();

			int total = n;
			if (total % 10000 == 0) {
				synchronized (values) {
					System.out.printf(endpoint + " #total %d, RPCs/sec %.1f Lag %d rpcs, avg RTT %.0f us\n", total, +total / (Sys.currentTime() - T0), (values.isEmpty() ? 0 : (n - values.first())), sumRTT / totRTT);
				}
			}
			while (values.size() > 1000)
				Threading.sleep(1);
		}
	}
	public static void main(String[] args) throws UnknownHostException {
		Log.setLevel(Level.ALL);

		String serverAddr = args.length > 0 ? args[0] : "localhost";

		sys.Sys.init();

		new RpcClient().doIt(serverAddr);
	}
}
