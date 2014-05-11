package swift.pubsub;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.proto.SwiftProtocolHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;
import sys.pubsub.PubSubNotification;

public class SwiftNotification extends PubSubNotification<CRDTIdentifier> {

    long seqN;
    String src;
    CausalityClock dcVersion;

    SwiftNotification() {
    }

    public SwiftNotification(String src, CausalityClock dcVersion, Notifyable<CRDTIdentifier> payload) {
        super(payload);
        this.src = src;
        this.dcVersion = dcVersion;
    }

    public SwiftNotification(Notifyable<CRDTIdentifier> payload) {
        super(payload);
    }

    public SwiftNotification(long seqN, String src, CausalityClock dcVersion, Notifyable<CRDTIdentifier> payload) {
        super(payload);
        this.src = src;
        this.seqN = seqN;
        this.dcVersion = dcVersion;
    }

    SwiftNotification clone(long seqN) {
        return new SwiftNotification(seqN, src, dcVersion, payload());
    }

    public String src() {
        return src;
    }

    public CausalityClock dcVersion() {
        return dcVersion;
    }

    public long seqN() {
        return seqN;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(handle, this);
    }

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        if (payload().key() != null)
            for (Subscriber<CRDTIdentifier> i : pubsub.subscribers(payload().key(), true))
                try {
                    ((SwiftSubscriber) i).onNotification(this);
                } finally {
                }
        else
            for (Subscriber<CRDTIdentifier> i : pubsub.subscribers(payload().keys()))
                try {
                    ((SwiftSubscriber) i).onNotification(this);
                } finally {
                }

    }
}
