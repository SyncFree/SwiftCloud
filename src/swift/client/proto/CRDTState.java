package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDT;

public class CRDTState {
    // TODO: shalln't we use CRDT class simply and allow leaving certain fields null?
    protected CRDT<?> crdt;
    protected CausalityClock version;

    // Fake constructor for Kryo serialization. Do NOT use.
    public CRDTState() {
    }
    
    public CRDTState(CRDT<?> crdt, CausalityClock version) {
        super();
        this.crdt = crdt;
        this.version = version;
    }

    public CRDT<?> getCrdt() {
        return crdt;
    }

    public CausalityClock getVersion() {
        return version;
    }
}
