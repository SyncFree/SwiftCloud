package swift.pubsub;

import java.util.concurrent.Executor;

import swift.crdt.CRDTIdentifier;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.pubsub.RemoteSubscriber;
import sys.pubsub.impl.AbstractPubSub;

public class DataServerPubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {

    final Executor executor;
    final RpcEndpoint endpoint;

    public DataServerPubSubService(Executor executor, RpcEndpoint endpoint) {
        this.endpoint = endpoint;
        this.executor = executor;
    }

    @Override
    public void publish(final Notifyable<CRDTIdentifier> info) {
        executor.execute(new Runnable() {
            public void run() {
                info.notifyTo(DataServerPubSubService.this);
            }
        });
    }

    synchronized public void subscribe(CRDTIdentifier key, Endpoint remote) {
        super.subscribe(key, new RemoteSubscriber("surrogate-" + remote, endpoint, remote));
    }

    synchronized public void unsubscribe(CRDTIdentifier key, Endpoint remote) {
        super.unsubscribe(key, new RemoteSubscriber("surrogate-" + remote, endpoint, remote));
    }

    @Override
    public void onNotification(UpdateNotification update) {
        Thread.dumpStack();
    }

    @Override
    public void onNotification(SnapshotNotification snapshot) {
        Thread.dumpStack();
    }
}
