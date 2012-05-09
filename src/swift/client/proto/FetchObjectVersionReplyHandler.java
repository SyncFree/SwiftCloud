package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link FetchObjectVersionReply}.
 * 
 * @author mzawirski
 */
public abstract class FetchObjectVersionReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcConnection conn, FetchObjectVersionReply reply);
}
