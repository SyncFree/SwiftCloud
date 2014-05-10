package swift.pubsub;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.dc.DCSurrogate;
import swift.proto.SwiftProtocolHandler;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.pubsub.impl.AbstractPubSub;

public class DataServerPubSubService extends AbstractPubSub<CRDTIdentifier> {

    final Executor executor;
    final RpcEndpoint endpoint;
    final DCSurrogate surrogate;

    public DataServerPubSubService(String id, Executor executor, DCSurrogate surrogate) {
        super(id);
        this.executor = executor;
        this.surrogate = surrogate;
        this.endpoint = Networking.rpcConnect().toDefaultService();
    }

    @Override
    synchronized public void publish(final Notifyable<CRDTIdentifier> info) {
        CausalityClock vrs = surrogate.getEstimatedDCVersionCopy();
        vrs.trim();
        final SwiftNotification evt = new SwiftNotification(vrs, info);

        for (Subscriber<CRDTIdentifier> i : subscribers(info.key(), true)) {
            i.onNotification(evt);
        }

        // executor.execute(new Runnable() {
        // public void run() {
        // }
        // });
    }

    synchronized public void subscribe(CRDTIdentifier key, Endpoint remote) {
        RemoteSwiftSubscriber rs = remoteSubscribers.get(remote);
        if (rs == null)
            remoteSubscribers.put(remote, rs = new RemoteSwiftSubscriber("surrogate-" + remote, endpoint, remote));
        super.subscribe(key, rs);
    }

    // There should be just one subscriber of this kind - the surrogatePubSub.
    synchronized public void subscribe(String clientId, CRDTIdentifier key, Subscriber<CRDTIdentifier> subscriber) {
        if (suPubSubAdapter == null)
            suPubSubAdapter = new SurrogateSubscriberAdapter(clientId);

        super.subscribe(key, suPubSubAdapter);
    }

    synchronized public void unsubscribe(String clientId, CRDTIdentifier key, Subscriber<CRDTIdentifier> subscriber) {
        // not implemented...
    }

    synchronized public void unsubscribe(CRDTIdentifier key, Endpoint remote) {
        // not implemented...

        // RemoteSwiftSubscriber rs = remoteSubscribers.get(remote);
        // if (rs != null)
        // super.unsubscribe(key, rs);
    }

    class SurrogateSubscriberAdapter implements Subscriber<CRDTIdentifier> {

        final String clientId;
        final SwiftProtocolHandler handler;
        final AtomicLong fifoSeq = new AtomicLong(0L);

        public SurrogateSubscriberAdapter(String clientId) {
            this.clientId = clientId;
            this.handler = surrogate.suPubSub.handler;
        }

        @Override
        public String id() {
            return clientId;
        }

        @Override
        public void onNotification(Notifyable<CRDTIdentifier> info) {
            handler.onReceive(null, ((SwiftNotification) info).clone(fifoSeq.incrementAndGet()));
        }
    }

    SurrogateSubscriberAdapter suPubSubAdapter;
    Map<Endpoint, RemoteSwiftSubscriber> remoteSubscribers = new HashMap<Endpoint, RemoteSwiftSubscriber>();
}
