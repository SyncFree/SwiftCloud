package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link CommitUpdatesReply}.
 * 
 * @author mzawirski
 */
public interface CommitUpdatesReplyHandler extends RpcHandler {
    public void onReceive(RpcConnection conn, CommitUpdatesReply reply);
}
