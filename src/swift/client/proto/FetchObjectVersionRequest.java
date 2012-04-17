package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionRequest extends ClientRequest {
    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected SubscriptionType subscriptionType;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public FetchObjectVersionRequest() {
    }

    /**
     * @deprecated
     */
    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            boolean subscribeUpdates) {
        this(clientId, uid, version, subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE);
    }

    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            SubscriptionType subscribeUpdates) {
        super(clientId);
        this.uid = uid;
        this.version = version;
        this.subscriptionType = subscribeUpdates;
    }

    /**
     * @return id of the requested object
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    /**
     * @return minimum version requested; null if client requests the most
     *         recent version
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return the subscription type for the object
     */
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
