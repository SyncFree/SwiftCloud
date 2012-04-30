package swift.benchmarks.rpc;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * 
 * @author smd
 * 
 */
abstract public class Handler extends AbstractRpcHandler {

	public void onReceive(final RpcConnection conn, final Request r) {
		Thread.dumpStack();
	}

	public void onReceive(final Reply r) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcConnection conn, final Reply r) {
		Thread.dumpStack();
	}
}
