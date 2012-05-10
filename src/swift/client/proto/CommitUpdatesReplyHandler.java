package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link CommitUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class CommitUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, CommitUpdatesReply reply);
}
