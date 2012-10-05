package swift.dc.proto;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

/**
 * RPC handler for {@link SeqCommitUpdatesReply}.
 * 
 * @author preguia
 */
public abstract class SeqCommitUpdatesReplyHandler extends AbstractRpcHandler {
    public abstract void onReceive(RpcHandle conn, SeqCommitUpdatesReply reply);
    
}


