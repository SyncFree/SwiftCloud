package swift.client.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link RecentUpdatesReply}.
 * 
 * @author mzawirski
 */
public abstract class RecentUpdatesReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, RecentUpdatesReply reply);
}
