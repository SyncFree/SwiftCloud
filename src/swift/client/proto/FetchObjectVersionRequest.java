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
    public enum SubscriptionType {
        /**
         * Receive updates on changes.
         */
        UPDATES,
        /**
         * Receive a single notification on changes.
         */
        NOTIFICATION,
        /**
         * Receive nothing on changes.
         */
        NONE
    }

    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected SubscriptionType subscribeUpdatesRequest;

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
        this( clientId, uid, version, subscribeUpdates ? SubscriptionType.UPDATES: SubscriptionType.NONE);
    }
    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            SubscriptionType subscribeUpdates) {
        super(clientId);
        this.uid = uid;
        this.version = version;
        this.subscribeUpdatesRequest = subscribeUpdates;
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
    public SubscriptionType isSubscribeUpdatesRequest() {
        return subscribeUpdatesRequest;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
