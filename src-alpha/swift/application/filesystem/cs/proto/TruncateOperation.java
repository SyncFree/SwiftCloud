package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class TruncateOperation extends FuseRemoteOperation {

    String path;
    long mode;


    TruncateOperation() {
    }

    public TruncateOperation(String path, long mode) {
        this.path = path;
        this.mode = mode;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).truncate(path, mode);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }


}
