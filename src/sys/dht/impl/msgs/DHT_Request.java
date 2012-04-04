package sys.dht.impl.msgs;

import sys.dht.api.DHT;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class DHT_Request implements RpcMessage {

	public DHT.Key key;
	public long handlerId;
	public DHT.Message payload;
	public Endpoint srcEndpoint;

	DHT_Request() {
	}

	public DHT_Request(DHT.Key key, DHT.Message payload) {
		this(key, payload, 0, null);
	}

	public DHT_Request(DHT.Key key, DHT.Message payload, long handlerId, Endpoint srcEndpoint) {
		this.key = key;
		this.payload = payload;
		this.handlerId = handlerId;
		this.srcEndpoint = srcEndpoint;
	}

	@Override
	public void deliverTo(RpcConnection conn, RpcHandler handler) {
		((DHT_StubHandler) handler).onReceive(conn, this);
	}

}
