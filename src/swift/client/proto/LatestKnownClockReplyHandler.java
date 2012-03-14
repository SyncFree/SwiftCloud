package swift.client.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link LatestKnownClockReply}.
 * 
 * @author mzawirski
 */
public abstract class LatestKnownClockReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, LatestKnownClockReply reply);
}
