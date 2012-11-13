package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link FastRecentUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class FastRecentUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, FastRecentUpdatesReply reply);
}
