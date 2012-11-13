package swift.application.filesystem.cs.proto;

import java.nio.ByteBuffer;
import java.util.Arrays;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import fuse.FuseException;

public class GetXAttrOperation extends FuseRemoteOperation {

    String path;
    String name;
    int dstCapacity; 
    int position;
    
    GetXAttrOperation() {        
    }
    
    public GetXAttrOperation(String path, String name, ByteBuffer dst, int position) {
        this.path = path;
        this.name = name;
        this.dstCapacity = dst.remaining();
        this.position = position;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            ByteBuffer tmp = ByteBuffer.allocate( dstCapacity );
            int res = ((RemoteFuseOperationHandler)handler).getxattr(path, name, tmp, position);
            handle.reply( new GetXAttrOperationResult( res, tmp ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
    
    static class GetXAttrOperationResult extends FuseOperationResult {
        
        byte[] data;
        
        GetXAttrOperationResult() {            
        }
        
        public GetXAttrOperationResult( int ret, ByteBuffer data ) {
            super( ret ) ;
            this.data = Arrays.copyOf( data.array(), data.position() ) ;
        }
        
        public void applyTo( ByteBuffer dst ) {
            dst.put( data ) ;
        }
    }
}
