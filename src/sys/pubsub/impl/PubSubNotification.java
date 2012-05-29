package sys.pubsub.impl;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class PubSubNotification implements RpcMessage {

	String group;
	Object payload;

	public PubSubNotification() {
	}

	PubSubNotification(String group, Object payload) {
		this.group = group;
		this.payload = payload;
	}

	public String group() {
		return group;
	}

	@SuppressWarnings("unchecked")
	public <T> T payload() {
		return (T) payload;
	}

	@Override
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((PubSubRpcHandler) handler).onReceive(conn, this);
	}

}
