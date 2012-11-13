package sys.net.impl;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.AbstractRpcHandler;

/**
 * 
 * @author smd
 * 
 */
abstract public class Handler extends AbstractRpcHandler {


	public void onReceive(final Reply r) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle handle, final Reply r) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle handle, final Request r) {
		Thread.dumpStack();
	}
}
