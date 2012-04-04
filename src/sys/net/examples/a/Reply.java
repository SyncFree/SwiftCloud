package sys.net.examples.a;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * 
 * @author smd
 * 
 */
public class Reply implements RpcMessage {

	Reply() {
	}

	@Override
	public void deliverTo(RpcConnection conn, RpcHandler handler) {
		if (conn.expectingReply())
			((Handler) handler).onReceive(conn, this);
		else
			((Handler) handler).onReceive(this);
	}
}
