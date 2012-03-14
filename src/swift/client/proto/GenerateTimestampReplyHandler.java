package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * RPC handler for {@link GenerateTimestampReply}.
 * 
 * @author mzawirski
 */
public interface GenerateTimestampReplyHandler extends RpcHandler {
    void onReceive(RpcConnection conn, GenerateTimestampReply reply);
}
