package sys.net.api.rpc;

/**
 * 
 * A convenience class for creating anonymous classes for processing invocations
 * involving specific message classes
 * 
 * @author smd
 * 
 */
abstract public class AbstractRpcHandler implements RpcHandler {

	@Override
	public void onFailure(final RpcHandle h) {
		Thread.dumpStack();
	}

	@Override
	public void onReceive(final RpcMessage m) {
		Thread.dumpStack();
	}

	@Override
	public void onReceive(final RpcHandle h, final RpcMessage m) {
		Thread.dumpStack();
	}
}
