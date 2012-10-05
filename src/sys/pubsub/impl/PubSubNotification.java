package sys.pubsub.impl;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class PubSubNotification<K,P> implements RpcMessage {

	K key;
	P info;

	//for kryo
	PubSubNotification() {
	}

	PubSubNotification(K key, P info) {
		this.key = key;
		this.info = info;
	}

	public K key() {
		return key;
	}
	
	public P info() {
		return info;
	}

	@Override
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((PubSubRpcHandler) handler).onReceive(conn, this);
	}

}
