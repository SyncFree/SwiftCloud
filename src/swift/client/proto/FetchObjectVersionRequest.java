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
    protected boolean committedVersionRequested;
    protected SubscriptionType subscriptionType;
    private boolean recentUpdatesRequested;

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
        this(clientId, uid, version, true, true, subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE);
    }

    public FetchObjectVersionRequest(String clientId, CRDTIdentifier uid, CausalityClock version,
            boolean committedVersionRequested, boolean recentUpdatesRequested, SubscriptionType subscribeUpdates) {
        super(clientId);
        this.uid = uid;
        this.version = version;
        this.committedVersionRequested = committedVersionRequested;
        this.recentUpdatesRequested = recentUpdatesRequested;
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
     * @return true if client wishes to receive more recent updates too
     */
    public boolean isRecentUpdatesRequsted() {
        return recentUpdatesRequested;
    }

    /**
     * @return true if the returned version must be committed
     */
    public boolean isCommittedVersionRequested() {
        return committedVersionRequested;
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
