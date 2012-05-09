package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link RecentUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class RecentUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcConnection conn, RecentUpdatesReply reply);
}
