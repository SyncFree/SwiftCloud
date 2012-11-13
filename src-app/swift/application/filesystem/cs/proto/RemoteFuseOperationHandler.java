package swift.application.filesystem.cs.proto;

import swift.application.filesystem.fuse.FilesystemFuse;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public abstract class RemoteFuseOperationHandler extends FilesystemFuse implements RpcHandler {
    
    @Override
    public void onReceive(RpcMessage m) {
        Thread.dumpStack();
    }

    @Override
    public void onReceive(RpcHandle handle, RpcMessage m) {
        Thread.dumpStack();
    }

    @Override
    public void onFailure(RpcHandle handle) {
        Thread.dumpStack();
    }
}
