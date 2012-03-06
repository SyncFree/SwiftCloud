package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;

public class FetchObjectVersionRequest {
    private CRDTIdentifier uid;
    private CausalityClock version;
    private boolean create;
    private boolean subscribeUpdates;
}
