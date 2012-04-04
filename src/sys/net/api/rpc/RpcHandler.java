package sys.net.api.rpc;

import sys.net.api.Endpoint;

/**
 * 
 * 
 * @author smd
 * 
 */
public interface RpcHandler {

	void onReceive(final RpcMessage m);

	void onReceive(final RpcConnection conn, final RpcMessage m);

	void onFailure();

	void onFailure(final Endpoint dst, final RpcMessage m);

}
