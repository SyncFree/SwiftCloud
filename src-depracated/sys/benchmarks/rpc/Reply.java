package sys.benchmarks.rpc;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import static sys.Sys.*;

import sys.net.api.*;
/**
 * 
 * @author smd
 * 
 */
public class Reply implements RpcMessage, CustomKryoSerializer {

    public int val;
    public double timestamp;
    
	public Reply() {
	}

	public Reply( int val, double ts) {
	    this.val = val;
	    this.timestamp = ts;
    }

	double rtt() {
		return (Sys.currentTime() - timestamp) * 1000000;
	}
	
	public String toString() {
        return "reply " + val;
	}
	
	@Override
	public void deliverTo( RpcHandle handle, RpcHandler handler) {
		if (handle.expectingReply())
			((Handler) handler).onReceive(handle, this);
		else
			((Handler) handler).onReceive(this);
	}
}
