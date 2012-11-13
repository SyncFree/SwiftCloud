package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import fuse.FuseStatfsSetter;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class StatFsOperation extends FuseRemoteOperation {

    FuseStatfsSetter sfs;

    StatFsOperation() {
    }

    public StatFsOperation(FuseStatfsSetter sfs) {
        this.sfs = sfs;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).statfs( sfs );
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
