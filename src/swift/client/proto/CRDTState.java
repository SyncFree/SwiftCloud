package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDT;

public class CRDTState {
    // TODO: shalln't we use CRDT class simply and allow leaving certain fields null?
    protected CRDT<?, ?> crdt;
    protected CausalityClock version;
}
