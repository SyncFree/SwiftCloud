package sys.examples.rpc;

import static sys.net.api.Networking.Networking;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

public class RpcClient {

	public static void main(String[] args) {

		Sys.init();

		RpcEndpoint endpoint = Networking.rpcBind(0, null);
		final Endpoint server = Networking.resolve("localhost", RpcServer.PORT);

		for (;;) {
			System.out.println("Sending:" + Thread.currentThread());
			endpoint.send(server, new Request(), new Handler() {

				@Override
				public void onFailure( RpcHandle handle ) {
					System.out.println("Send failed...");
				}

				@Override
				public void onReceive(RpcHandle handle, Reply r) {
					System.out.println("Got: " + r + " from:" + handle.remoteEndpoint() + ":" + Thread.currentThread());
					handle.reply(new Reply());
				}

			}, 9000);
			Threading.sleep(15000);
		}

	}
}
