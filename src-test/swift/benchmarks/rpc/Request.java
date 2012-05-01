package swift.benchmarks.rpc;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * 
 * @author smd
 * 
 */
public class Request implements RpcMessage {

    int val;
    
	Request() {
	}

	public Request( int val ) {
	    this.val = val;
	}

	@Override
	public void deliverTo(RpcConnection conn, RpcHandler handler) {
		((Handler) handler).onReceive(conn, this);
	}
}
