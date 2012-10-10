package swift.application.swiftset.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;


public class ServerACK extends SwiftSetRpc {

    public long serial;
    
    ServerACK(){    
    }
    
    public ServerACK( SwiftSetRpc rpc ){
        this.serial = rpc.serial;
    }
    
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((AppRpcHandler)handler).onReceive(this);           
    }
}
