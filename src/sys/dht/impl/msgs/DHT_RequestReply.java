package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_RequestReply implements RpcMessage {

	public DHT.Reply payload;

	DHT_RequestReply() {
	}

	public DHT_RequestReply(DHT.Reply payload) {
		this.payload = payload;
	}

	@Override
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(conn, this);
	}

}
