package sys.pubsub.impl;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class PubSubRpcHandler implements RpcHandler {

	@Override
	public void onReceive(RpcMessage m) {
		Thread.dumpStack();
	}

	@Override
	public void onFailure(RpcHandle handle) {
		Thread.dumpStack();
	}

	@Override
	public void onReceive(RpcHandle conn, RpcMessage m) {
		Thread.dumpStack();
	}

	public void onReceive(RpcHandle conn, PubSubAck m) {
		Thread.dumpStack();
	}

	public void onReceive(RpcHandle conn, PubSubNotification m) {
		Thread.dumpStack();
	}
}
