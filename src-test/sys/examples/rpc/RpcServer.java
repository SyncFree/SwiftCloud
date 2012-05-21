package sys.examples.rpc;

import java.util.logging.Level;

import sys.Sys;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Log;
import static sys.net.api.Networking.*;

public class RpcServer extends Handler {

	final static int PORT = 9999;

	RpcEndpoint endpoint;

	RpcServer() {
		endpoint = Networking.rpcBind(PORT, TransportProvider.NETTY_IO_TCP).toService(0, this);
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
        Log.setLevel("", Level.ALL);
        Log.setLevel("sys.dht.catadupa", Level.ALL);
        Log.setLevel("sys.dht", Level.ALL);
        Log.setLevel("sys.net", Level.ALL);
        Log.setLevel("sys", Level.ALL);

		Sys.init();

		new RpcServer();
	}
}
