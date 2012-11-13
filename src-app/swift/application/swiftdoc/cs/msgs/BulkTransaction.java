package swift.application.swiftdoc.cs.msgs;

import java.util.ArrayList;
import java.util.List;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class BulkTransaction extends SwiftDocRpc {

    public List<SwiftDocRpc> ops;

    BulkTransaction() {    
    }
    
    public BulkTransaction( List<SwiftDocRpc> ops ) {    
        this.ops = new ArrayList<SwiftDocRpc>( ops );
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((AppRpcHandler) handler).onReceive(handle, this);
    } 
}
