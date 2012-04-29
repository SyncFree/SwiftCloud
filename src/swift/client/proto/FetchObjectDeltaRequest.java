package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to get a delta between a known version and a specified version
 * of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectDeltaRequest extends FetchObjectVersionRequest {
    protected CausalityClock knownVersion;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public FetchObjectDeltaRequest() {
    }

    public FetchObjectDeltaRequest(String clientId, CRDTIdentifier id, CausalityClock knownVersion,
            CausalityClock version, boolean strictAvailableVersion, SubscriptionType subscribeUpdates) {
        super(clientId, id, version, strictAvailableVersion, subscribeUpdates);
        this.knownVersion = knownVersion;
    }

    /**
     * @return the latest version known by the client
     */
    public CausalityClock getKnownVersion() {
        return knownVersion;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
