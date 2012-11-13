package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link KeepaliveReply}.
 * 
 * @author mzawirski
 */
public abstract class KeepaliveReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, KeepaliveReply reply);
}
