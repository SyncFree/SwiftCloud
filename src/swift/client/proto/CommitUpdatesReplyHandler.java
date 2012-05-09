package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link CommitUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class CommitUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcConnection conn, CommitUpdatesReply reply);
}
