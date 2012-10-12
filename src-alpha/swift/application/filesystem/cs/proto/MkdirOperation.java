package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class MkdirOperation extends FuseRemoteOperation {

    String path;
    int mode;


    MkdirOperation() {        
    }
    
    public MkdirOperation(String path, int mode) {
        this.path = path;
        this.mode = mode;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).mkdir(path, mode);
            handle.reply( new FuseOperationResult(res));           
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
