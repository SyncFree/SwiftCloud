package swift.dc;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDT;

public class CRDTObject<V extends CRDT<V>> {
    /**
     * crdt object
     */
    CRDT<V> crdt;
    /**
     * current clock reflects all updates and their causal past
     */
    CausalityClock clock;
    /**
     * prune clock reflects the updates that have been discarded, making it
     * impossible to access a snapshot that is dominated by this clock
     */
    CausalityClock pruneClock;

    public CRDTObject() {
        // do nothing
    }
    public CRDTObject(CRDTData<V> data) {
        this.crdt = data.crdt.copy();
        this.clock = data.clock.clone();
        this.pruneClock = data.pruneClock.clone();
    }
}