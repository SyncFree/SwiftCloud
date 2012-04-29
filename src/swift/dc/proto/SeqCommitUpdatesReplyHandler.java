package swift.dc.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcConnection;

/**
 * RPC handler for {@link SeqCommitUpdatesReply}.
 * 
 * @author pregucia
 */
public abstract class SeqCommitUpdatesReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcConnection conn, SeqCommitUpdatesReply reply);
}
