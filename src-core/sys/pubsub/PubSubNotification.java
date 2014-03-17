package sys.pubsub;

import swift.crdt.core.CRDTIdentifier;
import swift.proto.SwiftProtocolHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.pubsub.PubSub.Notifyable;

public class PubSubNotification implements RpcMessage {

    int seqN;
    Notifyable<CRDTIdentifier> payload;

    protected PubSubNotification() {
    }

    public PubSubNotification(int seqN, Notifyable<CRDTIdentifier> payload) {
        this.seqN = seqN;
        this.payload = payload;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(handle, this);
    }

    public int seqN() {
        return seqN;
    }

    public Notifyable<CRDTIdentifier> payload() {
        return payload;
    }
}
