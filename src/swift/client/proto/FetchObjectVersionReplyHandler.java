package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link FetchObjectVersionReply}.
 * 
 * @author mzawirski
 */
public abstract class FetchObjectVersionReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, FetchObjectVersionReply reply);
}
