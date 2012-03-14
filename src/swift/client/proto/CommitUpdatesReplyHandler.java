package swift.client.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link CommitUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class CommitUpdatesReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, CommitUpdatesReply reply);
}
