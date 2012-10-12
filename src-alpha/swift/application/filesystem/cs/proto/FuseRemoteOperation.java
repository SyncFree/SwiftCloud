package swift.application.filesystem.cs.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class FuseRemoteOperation implements RpcMessage {

    //for kryo
    FuseRemoteOperation() {    
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((RemoteFuseOperationHandler)handler).onReceive(handle, this);
    }

}
