package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * Swift Client RPC interface for server -> client interaction.
 * <p>
 * For details, see message definitions.
 * 
 * @author mzawirski
 */
public interface SwiftClient {
    /**
     * 
     * @param conn
     * @param notification
     */
    void onReceive(RpcConnection conn, UpdatesNotification notification);
}
