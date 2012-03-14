package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link KeepaliveReply}.
 * 
 * @author mzawirski
 */
public interface KeepaliveReplyHandler extends RpcHandler {
    void onReceive(RpcConnection conn, KeepaliveReply reply);
}
