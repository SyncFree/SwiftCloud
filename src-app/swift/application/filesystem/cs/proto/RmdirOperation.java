package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class RmdirOperation extends FuseRemoteOperation {

    String path;

    RmdirOperation() {
    }


    public RmdirOperation(String path) {
        this.path = path;
    }


    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).rmdir(path);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
