package swift.application.filesystem.cs.proto;

import java.nio.CharBuffer;
import java.util.Arrays;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import fuse.FuseException;

public class ReadLinkOperation extends FuseRemoteOperation {

    String path;
    int linkCapacity;

    ReadLinkOperation() {
    }

    public ReadLinkOperation(String path, CharBuffer link) {
        this.path = path;
        this.linkCapacity = link.remaining();
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            CharBuffer tmp = CharBuffer.allocate(linkCapacity);
            int res = ((RemoteFuseOperationHandler) handler).readlink(path, tmp);
            handle.reply(new Result(res, tmp));
        } catch (FuseException e) {
            handle.reply(new FuseOperationResult());
        }
    }

    public static class Result extends FuseOperationResult {

        char[] data;

        Result() {
        }

        public Result(int ret, CharBuffer data) {
            super(ret);
            this.data = Arrays.copyOf(data.array(), data.position());
        }

        public void applyTo(CharBuffer dst) {
            dst.put(data);
        }
    }
}
