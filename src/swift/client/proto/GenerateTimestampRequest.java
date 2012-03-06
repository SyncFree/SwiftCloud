package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;

public class GenerateTimestampRequest {
    private CRDTIdentifier uid;
    private CausalityClock dominatedClock;
}
