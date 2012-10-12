package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class ChownOperation extends FuseRemoteOperation {

    String path;
    int uid;
    int gid;
    
    ChownOperation() {        
    }
    
    public ChownOperation( String path, int uid, int gid ) {
        this.path = path;
        this.uid = uid;
        this.gid = gid;        
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).chown(path, uid, gid);
            handle.reply( new FuseOperationResult( res ) );
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }

}
