package swift.client.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link UpdatesNotificationReply}.
 * 
 * @author mzawirski
 */
public abstract class UpdatesNotificationReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, UpdatesNotificationReply reply);
}
