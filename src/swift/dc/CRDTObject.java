package swift.dc;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
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
    public CRDTObject(CRDTData<V> data, CausalityClock version, String cltId) {
//      1) let int clientTxs =
//      clientTxClockService.getAndLockNumberOfCommitedTxs(clientId) //
//      probably it could be done better, lock-free
//      2)
//      let crdtCopy = retrieve(oid).copy()
//      crdtCopy.augumentWithScoutClock(new Timestamp(clientId, clientTxs))
//      3) clientTxClockService.unlock(clientId)
//      4) return crdtCopy
        if( DCDataServer.prune) {
            CMP_CLOCK cmp = version.compareTo( data.pruneClock);
            if( cmp == CMP_CLOCK.CMP_EQUALS || cmp == CMP_CLOCK.CMP_DOMINATES)
                this.crdt = data.prunedCrdt.copy();
            else
                this.crdt = data.crdt.copy();
        } else;
            this.crdt = data.crdt.copy();
        this.clock = data.clock.clone();
        this.pruneClock = data.pruneClock.clone();
        this.crdt.augmentWithScoutClock( data.cltClock.getLatest(cltId));
    }
}