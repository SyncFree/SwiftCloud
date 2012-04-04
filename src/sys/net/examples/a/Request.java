package sys.net.examples.a;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * 
 * @author smd
 * 
 */
public class Request implements RpcMessage {

	Request() {
	}

	@Override
	public void deliverTo(RpcConnection conn, RpcHandler handler) {
		((Handler) handler).onReceive(conn, this);
	}
}
