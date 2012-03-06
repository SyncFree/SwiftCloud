package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;

public class FetchObjectDeltaRequest {
    private CRDTIdentifier uid;
    private CausalityClock knownVersion;
    private CausalityClock requestedVersion;
    private boolean subscribeUpdates;
}
