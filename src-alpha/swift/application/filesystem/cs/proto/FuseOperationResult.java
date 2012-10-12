package swift.application.filesystem.cs.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class FuseOperationResult extends FuseRemoteOperation {

    int result;
    Throwable exception;

    public int intResult() {
        return result;
    }
    
    FuseOperationResult() {        
    }
    
    public FuseOperationResult( int result) {
        this.result = result;
        this.exception = null;
    }
    
    public FuseOperationResult( Throwable t ) {
        this.result = -1;
        this.exception = t;
    }
        
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((FuseResultHandler)handler).onReceive( this );
    }
    
    public String toString() {
        return String.format("ret=%s", result);
    }
}
