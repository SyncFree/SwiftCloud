package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link GenerateTimestampReply}.
 * 
 * @author mzawirski
 */
public abstract class GenerateTimestampReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, GenerateTimestampReply reply);
}
