package swift.dc.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link CommitTSReply}.
 * 
 * @author preguica
 */
public abstract class CommitTSReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcHandle conn, CommitTSReply reply);
}
