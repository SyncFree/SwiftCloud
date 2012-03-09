package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;

public class FetchObjectVersionRequest {
    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected boolean create;
    protected boolean subscribeUpdates;
}
