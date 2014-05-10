package sys.pubsub;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

abstract public class RemoteSubscriber<T> implements Subscriber<T> {

    final String id;
    final RpcEndpoint endpoint;

    protected Endpoint remote;

    public RemoteSubscriber(String id, RpcEndpoint endpoint, Endpoint remote) {
        this.id = id;
        this.remote = remote;
        this.endpoint = endpoint;
    }

    protected void setRemote(Endpoint remote) {
        this.remote = remote;
    }

    @Override
    public void onNotification(Notifyable<T> info) {
        try {
            if (remote != null && !id.equals(info.src()) || true) {
                endpoint.send(remote, (PubSubNotification<?>) info, RpcHandler.NONE, 0);
            }
        } finally {
        }
    }

    public Endpoint remoteEndpoint() {
        return remote;
    }

    public int hashCode() {
        return id.hashCode();
    }

    public boolean equals(Object other) {
        return other instanceof RemoteSubscriber && id.equals(((RemoteSubscriber<?>) other).id);
    }

    @Override
    public String id() {
        return id;
    }

}
