package sys.pubsub;

import java.util.concurrent.atomic.AtomicInteger;

import swift.crdt.CRDTIdentifier;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

public class RemoteSubscriber implements Subscriber<CRDTIdentifier> {

    final Object id;
    final Endpoint remote;
    final RpcEndpoint endpoint;
    final AtomicInteger seqN = new AtomicInteger();

    public RemoteSubscriber(String id, RpcEndpoint endpoint, Endpoint remote) {
        this.id = id;
        this.remote = remote;
        this.endpoint = endpoint;
    }

    @Override
    public void onNotification(Notifyable<CRDTIdentifier> info) {
        if (!id.equals(info.src())) {
            endpoint.send(remote, new PubSubNotification(seqN.getAndIncrement(), info), RpcHandler.NONE, 0);
        }
    }

    public Endpoint remoteEndpoint() {
        return remote;
    }

    public int hashCode() {
        return id.hashCode();
    }

    public boolean equals(Object other) {
        return other instanceof RemoteSubscriber && id.equals(((RemoteSubscriber) other).id);
    }
}
