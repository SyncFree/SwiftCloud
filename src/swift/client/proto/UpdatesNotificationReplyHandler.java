package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link UpdatesNotificationReply}.
 * 
 * @author mzawirski
 */
public interface UpdatesNotificationReplyHandler extends RpcHandler {
    void onReceive(RpcConnection conn, UpdatesNotificationReply reply);
}
