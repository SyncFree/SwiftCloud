package swift.client.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link GenerateTimestampReply}.
 * 
 * @author mzawirski
 */
public abstract class GenerateTimestampReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, GenerateTimestampReply reply);
}
