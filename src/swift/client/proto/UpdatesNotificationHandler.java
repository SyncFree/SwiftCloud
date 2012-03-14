package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link UpdatesNotification}.
 * 
 * @author mzawirski
 */
public interface UpdatesNotificationHandler extends RpcHandler {
    /**
     * 
     * @param conn
     * @param notification
     */
    void onReceive(RpcConnection conn, UpdatesNotification notification);
}
