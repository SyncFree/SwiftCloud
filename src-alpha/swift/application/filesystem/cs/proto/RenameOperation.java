package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class RenameOperation extends FuseRemoteOperation {

    private String from;
    private String to;

    RenameOperation() {
    }

    public RenameOperation(String from, String to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            int res = ((RemoteFuseOperationHandler)handler).rename(from, to);
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }


}
