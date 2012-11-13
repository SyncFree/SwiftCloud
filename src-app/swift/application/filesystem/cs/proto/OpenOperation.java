package swift.application.filesystem.cs.proto;

import fuse.FuseException;
import fuse.FuseOpenSetter;
import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class OpenOperation extends FuseRemoteOperation {

    String path;
    int flags;

    OpenOperation() {
    }

    public OpenOperation(String path, int flags, FuseOpenSetter openSetter) {
        this.path = path;
        this.flags = flags;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            Result result = new Result() ;
            int res = ((RemoteFuseOperationHandler) handler).open(path, flags, result);
            result.setResult(res) ;
            handle.reply( result );
        } catch (FuseException e) {
            e.printStackTrace();
            handle.reply(new FuseOperationResult());
        }
    }

    public static class Result extends FuseOperationResult implements FuseOpenSetter {

        Object fh;
        boolean isDirectIO;
        boolean keepInCache;
        
        Result() {
        }

        public void applyTo( FuseOpenSetter setter ) {
            setter.setFh( fh ) ;
            setter.setDirectIO( isDirectIO ) ;
            setter.setKeepCache( keepInCache );
        }
        
        public void setResult( int result ) {
            super.result = result;
        }
        
        @Override
        public boolean isDirectIO() {
            return isDirectIO;
        }

        @Override
        public boolean isKeepCache() {
            return keepInCache;
        }

        @Override
        public void setDirectIO(boolean directIO) {
            this.isDirectIO = directIO;
        }

        @Override
        public void setFh(Object fh) {
            this.fh = SwiftFuseServer.s2c_fh( fh);
        }

        @Override
        public void setKeepCache(boolean keepInCache) {
            this.keepInCache = keepInCache;                    
            Thread.dumpStack();
        }

        public String toString() {
            return String.format("%s (%s, %s, %s, %s)", getClass(), result, fh, isDirectIO, keepInCache);
        }
    }

    
}
