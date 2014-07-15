package sys.pubsub;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.impl.AbstractSubscriber;

public class RemoteSubscriber<T> extends AbstractSubscriber<T> {

    final RpcEndpoint endpoint;
    protected volatile Endpoint remote;
    public RemoteSubscriber(String id, RpcEndpoint endpoint, Endpoint remote) {
        super(id);
        this.remote = remote;
        this.endpoint = endpoint;
    }

    protected void setRemote(Endpoint remote) {
        this.remote = remote;
    }

    @Override
    public void onNotification(Notifyable<T> info) {
        try {
            if (remote != null) {
                endpoint.send(remote, (PubSubNotification<?>) info, RpcHandler.NONE, 0);
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public Endpoint remoteEndpoint() {
        return remote;
    }
}
