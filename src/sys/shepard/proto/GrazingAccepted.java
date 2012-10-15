package sys.shepard.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class GrazingAccepted implements RpcMessage {

    public GrazingAccepted() {        
    }
    
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
       ((ShepardProtoHandler)handler).onReceive(this);
    }
}
