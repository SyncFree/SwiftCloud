package sys.net.api.rpc;

import sys.net.api.Endpoint;
/**
 * 
 * A convenience class for creating anonymous classes for processing invocations involving specific message classes 
 * @author smd
 *
 */
abstract public class AbstractRpcHandler implements RpcHandler {

    public void onReceive(final RpcMessage m) {
        Thread.dumpStack();
    }

    public void onReceive(final RpcConnection conn, final RpcMessage m) {
        Thread.dumpStack();
    }

    public void onFailure() {
        Thread.dumpStack();
    }

    public void onFailure(final Endpoint dst, final RpcMessage m) {
        Thread.dumpStack();
    }

}
