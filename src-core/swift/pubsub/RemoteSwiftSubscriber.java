package swift.pubsub;

import java.util.concurrent.atomic.AtomicLong;

import swift.crdt.core.CRDTIdentifier;
import swift.dc.DCConstants;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.impl.RemoteEndpoint;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;
import sys.pubsub.RemoteSubscriber;

public class RemoteSwiftSubscriber extends RemoteSubscriber<CRDTIdentifier> implements SwiftSubscriber {
    AtomicLong fifoSeq = new AtomicLong(0L);

    public RemoteSwiftSubscriber(String clientId, RpcEndpoint endpoint) {
        super(clientId, endpoint, null);
    }

    public RemoteSwiftSubscriber(String clientId, RpcEndpoint endpoint, Endpoint remote) {
        super(clientId, endpoint, new RemoteEndpoint(remote.getHost(), DCConstants.PUBSUB_PORT));
    }

    public void onNotification(Notifyable<CRDTIdentifier> info) {
        super.onNotification(((SwiftNotification) info).clone(fifoSeq.incrementAndGet()));
    }

    public int hashCode() {
        return id().hashCode();
    }

    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        Subscriber<Endpoint> other = (Subscriber<Endpoint>) o;
        return id().equals(other.id());
    }

    @Override
    public void onNotification(SwiftNotification event) {
        super.onNotification(event.clone(fifoSeq.incrementAndGet()));
    }

    @Override
    public void onNotification(UpdateNotification update) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(BatchUpdatesNotification snapshot) {
        Thread.dumpStack();
    }
}