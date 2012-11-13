package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class ChmodOperation extends FuseRemoteOperation {

    int mode;
    String path;
    
    ChmodOperation() {        
    }
    
    public ChmodOperation( String path, int mode) {        
        this.path = path;
        this.mode = mode;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).chmod(path, mode);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
