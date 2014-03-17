package swift.pubsub;

import java.util.concurrent.Executor;

import swift.crdt.core.CRDTIdentifier;
import swift.proto.SwiftProtocolHandler;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSubNotification;
import sys.pubsub.impl.AbstractPubSub;

public class SurrogatePubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {

    final Executor executor;
    final RpcEndpoint endpoint;

    public SurrogatePubSubService(Executor executor, RpcEndpoint endpoint) {
        this.executor = executor;
        this.endpoint = endpoint;
        this.endpoint.setHandler(new SwiftProtocolHandler() {
            @Override
            public void onReceive(RpcHandle conn, PubSubNotification p) {
                Thread.dumpStack();
                p.payload().notifyTo(SurrogatePubSubService.this);
            }
        });
    }

    // @Override
    // public void publish(Object key, Notifyable info) {
    // // 1) needs to broadcast to other surrogates
    // // 2) needs to notify interested scouts
    // // Thread.dumpStack();
    // }

    @Override
    public void publish(final Notifyable<CRDTIdentifier> info) {
        executor.execute(new Runnable() {
            public void run() {
                info.notifyTo(SurrogatePubSubService.this);
            }
        });
    }

    @Override
    public void onNotification(UpdateNotification update) {
        publish(update);
    }

    @Override
    public void onNotification(SnapshotNotification snapshot) {
        publish(snapshot);
    }
}
