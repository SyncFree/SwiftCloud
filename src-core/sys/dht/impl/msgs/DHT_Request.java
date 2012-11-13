package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_Request implements RpcMessage {

	public DHT.Key key;
	public boolean redirected;
	public DHT.Message payload;
	public boolean expectingReply;
	
	DHT_Request() {
	}

	public DHT_Request(DHT.Key key, DHT.Message payload) {
		this(key, payload, false);
	}

	public DHT_Request(DHT.Key key, DHT.Message payload, boolean expectingReply) {
		this.key = key;
		this.payload = payload;
		this.redirected = false;
		this.expectingReply = expectingReply;
	}

	
	@Override
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(conn, this);
	}

	public String toString() {
		return super.toString() ;
	}
	
}
