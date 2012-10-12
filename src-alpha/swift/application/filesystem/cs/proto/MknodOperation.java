package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class MknodOperation extends FuseRemoteOperation {

    String path;
    int mode;
    int rdev;
    
    MknodOperation() {    
    }
    
    
    public MknodOperation(String path, int mode, int rdev) {
        this.path = path;
        this.mode = mode;
        this.rdev = rdev;
    }


    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).mknod(path, mode, rdev);
            handle.reply( new FuseOperationResult(res));
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
