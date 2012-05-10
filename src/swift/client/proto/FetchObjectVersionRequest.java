package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionRequest extends ClientRequest {
    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected boolean strictUnprunedVersion;
    protected SubscriptionType subscriptionType;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public FetchObjectVersionRequest() {
    }

    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            final boolean strictUnprunedVersion, SubscriptionType subscribeUpdates) {
        super(clientId);
        this.uid = uid;
        this.version = version;
        this.strictUnprunedVersion = strictUnprunedVersion;
        this.subscriptionType = subscribeUpdates;
    }

    /**
     * @return id of the requested object
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    /**
     * @return minimum version requested
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return true strictly this (unpruned) version needs to be available in
     *         the reply; otherwise a more recent version is acceptable
     */
    public boolean isStrictAvailableVersion() {
        return strictUnprunedVersion;
    }

    /**
     * @return the subscription type for the object
     */
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
