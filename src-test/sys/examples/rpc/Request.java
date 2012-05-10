package sys.examples.rpc;

import sys.net.api.rpc.RpcHandle;
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
	public void deliverTo(RpcHandle conn, RpcHandler handler) {
		((Handler) handler).onReceive(conn, this);
	}
}
