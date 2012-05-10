package sys.dht.impl.msgs;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

abstract public class DHT_StubHandler extends AbstractRpcHandler {

	@Override
	public void onFailure( final RpcHandle handle) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_Request req) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_RequestReply reply) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_ReplyReply reply) {
		Thread.dumpStack();
	}

}
