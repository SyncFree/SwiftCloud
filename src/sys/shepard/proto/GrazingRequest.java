package sys.shepard.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class GrazingRequest implements RpcMessage {

    public GrazingRequest(){        
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
       ((ShepardProtoHandler)handler).onReceive(handle, this);
    }

}
