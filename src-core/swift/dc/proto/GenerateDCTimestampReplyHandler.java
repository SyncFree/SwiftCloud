package swift.dc.proto;

import swift.client.proto.SilentFailRpcHandler;
import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link GenerateDCTimestampReply}.
 * 
 * @author nmp
 */
public abstract class GenerateDCTimestampReplyHandler extends SilentFailRpcHandler {
    public abstract void onReceive(RpcHandle conn, GenerateDCTimestampReply reply);
}
