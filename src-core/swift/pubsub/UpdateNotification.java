package swift.pubsub;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.proto.ObjectUpdatesInfo;
import swift.proto.SwiftProtocolHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.PubSub.Subscriber;
import sys.pubsub.PubSubNotification;

public class UpdateNotification extends PubSubNotification<CRDTIdentifier> {

    // TODO: lots of redundant things on the wire?
    public ObjectUpdatesInfo info;
    public CausalityClock dcVersion;

    UpdateNotification() {
    }

    public UpdateNotification(Object srcId, ObjectUpdatesInfo info, CausalityClock dcVersion) {
        super(srcId);
        this.info = info;
        this.dcVersion = dcVersion;
    }

    @Override
    public CRDTIdentifier key() {
        return info.getId();
    }

    @Override
    public Set<CRDTIdentifier> keys() {
        return null;
    }

    public CausalityClock dcVersion() {
        return dcVersion;
    }

    @Override
    public void notifyTo(Subscriber<CRDTIdentifier> subscriber) {
        ((SwiftSubscriber) subscriber).onNotification(this);
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(this);
    }
}
