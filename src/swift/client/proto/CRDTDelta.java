package swift.client.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

public class CRDTDelta {
    private CRDTIdentifier uid;
    private CausalityClock dependency;
    private List<CRDTOperation> operations;
}
