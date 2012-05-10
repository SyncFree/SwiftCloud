package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_ReplyReply implements RpcMessage {

	public long handlerId;
	public long replyHandlerId;
	public DHT.Reply payload;

	DHT_ReplyReply() {
	}

	public DHT_ReplyReply(DHT.Reply payload, long handlerId) {
		this(payload, handlerId, 0);
	}

	public DHT_ReplyReply(DHT.Reply payload, long handlerId, long replyHandlerId) {
		this.payload = payload;
		this.handlerId = handlerId;
		this.replyHandlerId = replyHandlerId;
	}

	@Override
	public void deliverTo( RpcHandle handle, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(handle, this);
	}

}
