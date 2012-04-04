package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_RequestReply implements RpcMessage {

	public long handlerId;
	public long replyHandlerId;
	public DHT.Reply payload;

	DHT_RequestReply() {
	}

	public DHT_RequestReply(DHT.Reply payload, long handlerId) {
		this(payload, handlerId, 0);
	}

	public DHT_RequestReply(DHT.Reply payload, long handlerId, long replyHandlerId) {
		this.payload = payload;
		this.handlerId = handlerId;
		this.replyHandlerId = replyHandlerId;
	}

	@Override
	public void deliverTo(RpcConnection conn, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(conn, this);
	}

}
