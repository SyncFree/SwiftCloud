package swift.client.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link FetchObjectVersionReply}.
 * 
 * @author mzawirski
 */
public abstract class FetchObjectVersionReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, FetchObjectVersionReply reply);
}
