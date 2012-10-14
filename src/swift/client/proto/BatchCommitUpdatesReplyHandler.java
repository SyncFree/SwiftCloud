package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link BatchCommitUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class BatchCommitUpdatesReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, BatchCommitUpdatesReply reply);
}
