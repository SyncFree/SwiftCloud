package sys.examples.rpc;

import sys.net.api.rpc.RpcHandle;
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
	public void deliverTo( RpcHandle handle, RpcHandler handler) {
		if (handle.expectingReply())
			((Handler) handler).onReceive(handle, this);
		else
			((Handler) handler).onReceive(this);
	}
}
