package swift.proto;

import java.util.ArrayList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.impl.KryoLib;

public class ObjectUpdatesInfo {

    protected boolean dirty;
    protected CRDTIdentifier id;
    protected CausalityClock oldClock;
    protected CausalityClock newClock;
    protected CausalityClock pruneClock;
    protected List<CRDTObjectUpdatesGroup<?>> updates;

    public ObjectUpdatesInfo() {
    }

    public ObjectUpdatesInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
            CausalityClock pruneClock, CRDTObjectUpdatesGroup<?> update) {
        this.id = id;
        this.dirty = true;
        this.oldClock = oldClock;
        this.newClock = newClock;
        this.updates = new ArrayList<CRDTObjectUpdatesGroup<?>>();
        updates.add(update);
        this.pruneClock = pruneClock;
    }

    public ObjectUpdatesInfo(CRDTIdentifier id, CausalityClock oldClock, CausalityClock newClock,
            CausalityClock pruneClock, List<CRDTObjectUpdatesGroup<?>> updates, boolean dirty) {
        this.id = id;
        this.dirty = dirty;
        this.updates = updates;
        this.oldClock = oldClock;
        this.newClock = newClock;
        this.pruneClock = pruneClock;
    }

    /**
     * @return id of the object
     */
    public CRDTIdentifier getId() {
        return id;
    }

    /**
     * @return previous clock of the CRDT
     */
    public CausalityClock getOldClock() {
        return oldClock;
    }

    /**
     * @return current clock of the CRDT after submitting this changes
     */
    public CausalityClock getNewClock() {
        return newClock;
    }

    /**
     * @return true if there was any update to the object between
     *         {@link #getOldClock()} and {@link #getNewClock()}
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * @return list of all updates to the object between {@link #getOldClock()}
     *         and {@link #getNewClock()} or null if only an invalidation
     *         (notification) has been requested
     */
    public List<CRDTObjectUpdatesGroup<?>> getUpdates() {
        return updates;
    }

    public void clearOperations() {
        updates.clear();
    }

    public ObjectUpdatesInfo clone() {
        return KryoLib.copy(this);
    }

    // public ObjectUpdateInfo cloneNotification() {
    // return new ObjectSubscriptionInfo(id, oldClock, newClock, pruneClock,
    // null, dirty);
    // }

    public ObjectUpdatesInfo clone(Timestamp t) {
        CausalityClock newC = newClock.clone();
        if (t != null)
            newC.recordAllUntil(t);
        return new ObjectUpdatesInfo(id, oldClock, newC, pruneClock, updates, dirty);
    }

    public CausalityClock getPruneClock() {
        return pruneClock;
    }
}
