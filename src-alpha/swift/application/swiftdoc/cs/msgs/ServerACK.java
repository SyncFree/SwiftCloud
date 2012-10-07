package swift.application.swiftdoc.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;


public class ServerACK extends SwiftDocRpc {

    public long serial;
    
    ServerACK(){    
    }
    
    public ServerACK( SwiftDocRpc rpc ){
        this.serial = rpc.serial;
    }
    
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((AppRpcHandler)handler).onReceive(this);           
    }
}
