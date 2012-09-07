package sys.benchmarks.rpc;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

/**
 * 
 * @author smd
 * 
 */
abstract public class Handler extends AbstractRpcHandler {

	
	public void onReceive(final Reply r) {
		Thread.dumpStack();
	}
	
	public void onReceive(final RpcHandle handle, final Request r) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final Reply r) {
		Thread.dumpStack();
	}
}
