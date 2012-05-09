package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link FastRecentUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class FastRecentUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcConnection conn, FastRecentUpdatesReply reply);
}
