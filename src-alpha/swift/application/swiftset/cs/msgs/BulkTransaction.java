package swift.application.swiftset.cs.msgs;

import java.util.List;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class BulkTransaction extends SwiftSetRpc {

    public List<SwiftSetRpc> ops;

    BulkTransaction() {    
    }
    
    public BulkTransaction( List<SwiftSetRpc> ops ) {    
        this.ops = ops;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((AppRpcHandler) handler).onReceive(handle, this);
    } 
}
