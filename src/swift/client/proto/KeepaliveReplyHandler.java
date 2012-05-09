package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link KeepaliveReply}.
 * 
 * @author mzawirski
 */
public abstract class KeepaliveReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcConnection conn, KeepaliveReply reply);
}
