package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link LatestKnownClockReply}.
 * 
 * @author mzawirski
 */
public interface LatestKnownClockReplyHandler extends RpcHandler {
    void onReceive(RpcConnection conn, LatestKnownClockReply reply);
}
