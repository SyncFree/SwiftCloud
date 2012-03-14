package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link FetchObjectVersionReply}.
 * 
 * @author mzawirski
 */
public interface FetchObjectVersionReplyHandler extends RpcHandler {
    void onReceive(RpcConnection conn, FetchObjectVersionReply reply);
}
