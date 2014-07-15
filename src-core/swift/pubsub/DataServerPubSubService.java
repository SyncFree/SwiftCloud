package swift.pubsub;

import static sys.net.api.Networking.Networking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import swift.crdt.core.CRDTIdentifier;
import swift.dc.DCSurrogate;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.pubsub.RemoteSubscriber;
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

    public void subscribe(CRDTIdentifier key, Endpoint remote) {
        RemoteSubscriber<CRDTIdentifier> rs = remoteSubscribers.get(remote);
        if (rs == null) {
            remoteSubscribers.put(remote, rs = new RemoteSubscriber<CRDTIdentifier>("surrogate-" + remote, endpoint,
                    remote));
        }
        super.subscribe(key, rs);
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

    Map<Endpoint, RemoteSubscriber<CRDTIdentifier>> remoteSubscribers = new ConcurrentHashMap<Endpoint, RemoteSubscriber<CRDTIdentifier>>();
}
