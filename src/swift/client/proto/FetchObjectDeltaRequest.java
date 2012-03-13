package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;

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

    public FetchObjectDeltaRequest(CRDTIdentifier id, CausalityClock knownVersion, CausalityClock version,
            boolean subscribeUpdates) {
        super(id, version, subscribeUpdates);
        this.knownVersion = knownVersion;
    }

    /**
     * @return the latest version known by the client
     */
    public CausalityClock getKnownVersion() {
        return knownVersion;
    }
}
