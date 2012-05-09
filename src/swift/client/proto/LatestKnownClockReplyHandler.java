package swift.client.proto;

import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link LatestKnownClockReply}.
 * 
 * @author mzawirski
 */
public abstract class LatestKnownClockReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcConnection conn, LatestKnownClockReply reply);
}
