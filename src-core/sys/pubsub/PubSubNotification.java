package sys.pubsub;

import java.util.Set;

import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.pubsub.PubSub.Notifyable;

public class PubSubNotification<T> implements RpcMessage, Notifyable<T> {

    Notifyable<T> payload;

    protected PubSubNotification() {
    }

    public PubSubNotification(Notifyable<T> payload) {
        this.payload = payload;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        Thread.dumpStack();
    }

    public Notifyable<T> payload() {
        return payload;
    }

    public String toString() {
        return "" + payload;
    }

    @Override
    public Object src() {
        return payload.src();
    }

    @Override
    public T key() {
        return payload.key();
    }

    @Override
    public Set<T> keys() {
        return payload.keys();
    }

    @Override
    public void notifyTo(PubSub<T> pubsub) {
        Thread.dumpStack();
    }

    @Override
    public Timestamp timestamp() {
        return payload.timestamp();
    }
}
