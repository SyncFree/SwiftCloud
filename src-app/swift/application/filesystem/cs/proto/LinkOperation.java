package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import swift.dc.DHTDataNode.ReplyHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class LinkOperation extends FuseRemoteOperation {

    String from;
    String to;
    
    LinkOperation() {        
    }
    
    
    public LinkOperation(String from, String to) {
        this.from = from;
        this.to = to;
    }


    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).link(from, to);
            handle.reply( new FuseOperationResult(res));
            
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
