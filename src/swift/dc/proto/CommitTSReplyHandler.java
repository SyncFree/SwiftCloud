package swift.dc.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link CommitTSReply}.
 * 
 * @author preguica
 */
public abstract class CommitTSReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, CommitTSReply reply);
}
