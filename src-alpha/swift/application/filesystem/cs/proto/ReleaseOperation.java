package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class ReleaseOperation extends FuseRemoteOperation {

    private String path;
    private Object fileHandle;
    private int flags;

    ReleaseOperation(){    
    }

    public ReleaseOperation(String path, Object fileHandle, int flags) {
        this.path = path;
        this.fileHandle = fileHandle;
        this.flags = flags;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).release(path, SwiftFuseServer.c2s_fh( fileHandle), flags);
            SwiftFuseServer.disposeFh( fileHandle);            
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }


}
