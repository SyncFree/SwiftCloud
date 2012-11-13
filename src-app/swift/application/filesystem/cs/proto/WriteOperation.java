package swift.application.filesystem.cs.proto;

import java.nio.ByteBuffer;
import java.util.Arrays;

import fuse.FuseException;
import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class WriteOperation extends FuseRemoteOperation {
    public static int MAX_WRITE_SIZE = 4096;
    
    String path;
    Object fh;
    boolean isWritepage;
    byte[] buf;
    long offset;

    public WriteOperation() {
    }

    
    public WriteOperation(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) {
        this.path = path;
        this.fh = fh;
        this.offset = offset;
        this.isWritepage = isWritepage;
        
        int frameSize = Math.min( MAX_WRITE_SIZE, buf.limit() - buf.position() ) ;
        ByteBuffer tmp = ByteBuffer.allocate( frameSize ) ;
        
        while( tmp.hasRemaining() )
            tmp.put( buf.get() ) ;
        
        this.buf = tmp.array();
    }


    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            ByteBuffer tmp = ByteBuffer.wrap( buf ) ;
            int res = ((RemoteFuseOperationHandler)handler).write(path, SwiftFuseServer.c2s_fh( fh), isWritepage, tmp, offset);            
            handle.reply( new FuseOperationResult( res ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
}
