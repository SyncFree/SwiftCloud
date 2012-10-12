package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class FSyncOperation extends FuseRemoteOperation {
    
    String path;
    Object fileHandle;
    boolean isDatasync;
    
    FSyncOperation(){        
    }

    
    public FSyncOperation(String path, Object fileHandle, boolean isDatasync) {
        super();
        this.path = path;
        this.fileHandle = fileHandle;
        this.isDatasync = isDatasync;
    }


    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).fsync(path, SwiftFuseServer.c2s_fh( fileHandle), isDatasync);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }

}
