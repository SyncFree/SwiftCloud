package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class UnlinkOperation extends FuseRemoteOperation {

    String path;

    UnlinkOperation() {
    }

    public UnlinkOperation(String path) {
        this.path = path;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).unlink(path);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
