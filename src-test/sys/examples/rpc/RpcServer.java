package sys.examples.rpc;

import sys.Sys;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;

public class RpcServer extends Handler {

	final static int PORT = 9999;

	RpcEndpoint endpoint;

	RpcServer(RpcEndpoint e) {
		endpoint = e;
		endpoint.setHandler(this);
		System.out.println("Server ready...");
	}

	@Override
	public void onReceive(final RpcHandle handle, final Request req) {

		System.out.println("Got: " + req + " from: " + handle.remoteEndpoint() + ":" + handle.getClass());
		handle.reply(new Reply(), new Handler() {

			@Override
			public void onFailure( RpcHandle handle) {
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
