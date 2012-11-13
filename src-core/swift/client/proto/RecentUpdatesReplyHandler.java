package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link RecentUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class RecentUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, RecentUpdatesReply reply);
}
