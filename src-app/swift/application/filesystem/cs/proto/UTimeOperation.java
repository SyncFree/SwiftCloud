package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class UTimeOperation extends FuseRemoteOperation {

    String path;
    int atime;
    int mtime;

    UTimeOperation() {
    }

    public UTimeOperation(String path, int atime, int mtime) {
        this.path = path;
        this.atime = atime;
        this.mtime = mtime;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).utime(path, atime, mtime);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
