package sys.net.examples.a;

import sys.Sys;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

public class RpcServer extends Handler {

	final static int PORT = 9999;

	RpcEndpoint endpoint;

	RpcServer(RpcEndpoint e) {
		endpoint = e;
		endpoint.setHandler(this);
		System.out.println("Server ready...");
	}

	@Override
	public void onReceive(final RpcConnection conn, final Request req) {

		System.out.println("Got: " + req + " from: " + conn.remoteEndpoint() + ":" + Thread.currentThread());
		conn.reply(new Reply(), new Handler() {

			@Override
			public void onFailure() {
				System.out.println("reply failed...");
			}

			@Override
			public void onReceive(Reply r) {
				System.out.println("Got: " + r + ":" + Thread.currentThread());
			}
		}, 0);
	}

	/*
	 * The server class...
	 */
	public static void main(final String[] args) {

		Sys.init();

		new RpcServer(Networking.Networking.rpcBind(PORT, null));
	}
}
