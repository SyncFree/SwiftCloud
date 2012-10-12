package swift.application.filesystem.cs.proto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;

import fuse.FuseException;
import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class ReadOperation extends FuseRemoteOperation {

    private static int MAX_REMOTE_READ_SIZE = 1250;

    String path;
    Object fh;
    int capacity;
    long offset;

    ReadOperation() {
    }

    public ReadOperation(String path, Object fh, ByteBuffer buf, long offset) {
        this.path = path;
        this.fh = fh;
        this.offset = offset;
        this.capacity = buf.limit() - buf.position();
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            ByteBuffer tmp = ByteBuffer.allocate(capacity);
            int res = ((RemoteFuseOperationHandler) handler).read(path, SwiftFuseServer.c2s_fh( fh), tmp, offset);
            handle.reply(new Result(res, tmp));
        } catch (FuseException e) {
            handle.reply(new FuseOperationResult());
        }
    }

    public static class Result extends FuseOperationResult {

        byte[] data;

        Result() {
        }

        public Result(int ret, ByteBuffer data) {
            super(ret);
            data.flip();
            this.data = Arrays.copyOf(data.array(), data.limit() - data.position());
        }

        public void applyTo(ByteBuffer dst) {
            dst.put( data ) ;
        }
    }
}
