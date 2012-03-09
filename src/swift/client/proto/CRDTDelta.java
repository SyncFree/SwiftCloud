package swift.client.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

public abstract class CRDTDelta {
    // TODO: provide alternative: state-based propagation (useful/necessary for
    // fetch delta)
    protected CRDTIdentifier uid;
    protected CausalityClock dependency;
    protected CausalityClock version;
    protected List<CRDTOperation> operations;
}
