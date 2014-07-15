package sys.pubsub;

import swift.proto.SwiftProtocolHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.net.impl.KryoLib;
import sys.pubsub.PubSub.Notifyable;

abstract public class PubSubNotification<T> implements RpcMessage, Notifyable<T> {

    protected long seqN;
    protected Object src;

    protected PubSubNotification() {
    }

    protected PubSubNotification(Object src) {
        this.src = src;
        this.seqN = -1L;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(this);
    }

    public Notifyable<T> payload() {
        return this;
    }

    @Override
    public Object src() {
        return src;
    }

    @Override
    public long seqN() {
        return seqN;
    }

    public Notifyable<T> clone(long newSeqN) {
        PubSubNotification<T> res = KryoLib.copyShallow(this);
        res.seqN = newSeqN;
        return res;
    }
}
