package swift.application.filesystem.cs.proto;

import java.util.ArrayList;
import java.util.List;

import fuse.FuseDirFiller;
import fuse.FuseException;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class GetDirOperation extends FuseRemoteOperation {

    String path;
    
    GetDirOperation() {        
    }
    
    public GetDirOperation(String path) {
        this.path = path;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            
            _FuseDirFiller filler = new _FuseDirFiller();
            int res = ((RemoteFuseOperationHandler)handler).getdir(path, filler );
            handle.reply( new Result( res, filler ) ) ;
            
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
    
    public static class Result extends FuseOperationResult {
        
        _FuseDirFiller filler;
        
        Result() {            
        }
        
        public Result( int ret, _FuseDirFiller filler ) {
            super( ret ) ;
            this.filler = filler ;
        }
        
       
        public void applyTo( FuseDirFiller dstFiller ) {
            for( _FuseDirFillerItem i : filler.items )
                dstFiller.add( i.name, i.inode, i.mode);                
        }

    }
    
    static class _FuseDirFiller implements FuseDirFiller {

        List<_FuseDirFillerItem> items = new ArrayList<_FuseDirFillerItem>();
        
        public void add(String name, long inode, int mode) {
            items.add( new _FuseDirFillerItem(name, inode, mode) );
        }        
    }
    
    static class _FuseDirFillerItem  {
        
        String name;
        long inode;
        int mode;

        //for kryo
        _FuseDirFillerItem(){            
        }
        
        public _FuseDirFillerItem(String name, long inode, int mode) {
            this.name = name;
            this.inode = inode;
            this.mode = mode;
        }
        
    }
}
