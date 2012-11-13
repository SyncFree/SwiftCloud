package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class FlushOperation extends FuseRemoteOperation {

    String path;
    Object fileHandle;
    
    FlushOperation() {        
    }
    
    public FlushOperation(String path, Object handle) {
        this.path = path;
        this.fileHandle = handle;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
           int res =  ((RemoteFuseOperationHandler)handler).flush(path, SwiftFuseServer.c2s_fh( fileHandle) );
           handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
