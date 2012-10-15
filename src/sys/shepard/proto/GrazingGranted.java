package sys.shepard.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class GrazingGranted implements RpcMessage {

    int duration;

    public GrazingGranted() {
    }

    public GrazingGranted(int duration) {
        this.duration = duration;
    }

    public int duration() {
        return duration;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((ShepardProtoHandler) handler).onReceive(this);
    }

}
