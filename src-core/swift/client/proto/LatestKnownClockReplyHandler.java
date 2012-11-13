package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link LatestKnownClockReply}.
 * 
 * @author mzawirski
 */
public abstract class LatestKnownClockReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, LatestKnownClockReply reply);
}
