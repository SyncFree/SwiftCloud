package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_ResolveKey implements RpcMessage {

	public DHT.Key key;

	DHT_ResolveKey() {
	}

	public DHT_ResolveKey(DHT.Key key) {
		this.key = key;
	}

	@Override
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(conn, this);
	}

	public String toString() {
		return super.toString() + "  " + key ;
	}
	
}
