package sys.dht.impl.msgs;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

abstract public class DHT_StubHandler extends AbstractRpcHandler {

	@Override
	public void onFailure() {
		Thread.dumpStack();
	}

	public void onReceive(final RpcConnection conn, final DHT_Request req) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcConnection conn, final DHT_RequestReply reply) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcConnection conn, final DHT_ReplyReply reply) {
		Thread.dumpStack();
	}

}
