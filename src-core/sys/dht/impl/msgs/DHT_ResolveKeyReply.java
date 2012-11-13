package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_ResolveKeyReply implements RpcMessage {

	public DHT.Key key;
	public Endpoint endpoint;

	DHT_ResolveKeyReply() {
	}

	public DHT_ResolveKeyReply(DHT.Key key, Endpoint endpoint) {
		this.key = key;
		this.endpoint = endpoint;
	}

	@Override
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(conn, this);
	}

	public String toString() {
		return super.toString() + " :> " + key ;
	}
	
}
