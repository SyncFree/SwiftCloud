package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_ReplyReply implements RpcMessage {

	public DHT.Reply payload;

	DHT_ReplyReply() {
	}

	public DHT_ReplyReply(DHT.Reply payload) {
		this.payload = payload;
	}

	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(handle, this);
	}

}
