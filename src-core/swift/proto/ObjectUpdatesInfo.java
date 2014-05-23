package swift.proto;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.impl.KryoLib;

public class ObjectUpdatesInfo {

    protected boolean dirty;
    protected CRDTIdentifier id;
    protected CausalityClock pruneClock;
    protected List<CRDTObjectUpdatesGroup<?>> updates;

    public ObjectUpdatesInfo() {
    }

    public ObjectUpdatesInfo(CRDTIdentifier id, CausalityClock pruneClock, CRDTObjectUpdatesGroup<?> update) {
        this.id = id;
        this.dirty = true;
        this.updates = new ArrayList<CRDTObjectUpdatesGroup<?>>();
        updates.add(update.strippedWithCopiedTimestampMappings());
        this.pruneClock = pruneClock;
    }

    public ObjectUpdatesInfo(CRDTIdentifier id, CausalityClock pruneClock, List<CRDTObjectUpdatesGroup<?>> updates,
            boolean dirty) {
        this.id = id;
        this.dirty = dirty;
        this.updates = updates;
        this.pruneClock = pruneClock;
    }

    /**
     * @return id of the object
     */
    public CRDTIdentifier getId() {
        return id;
    }

    /**
     * @return list of all updates to the object between {@link #getOldClock()}
     *         and {@link #getNewClock()} or null if only an invalidation
     *         (notification) has been requested
     */
    public List<CRDTObjectUpdatesGroup<?>> getUpdates() {
        final LinkedList<CRDTObjectUpdatesGroup<?>> res = new LinkedList<CRDTObjectUpdatesGroup<?>>();
        for (final CRDTObjectUpdatesGroup<?> u : updates) {
            res.add(u.withId(this.id));
        }
        return res;
    }

    public void clearOperations() {
        updates.clear();
    }

    public ObjectUpdatesInfo clone() {
        return KryoLib.copy(this);
    }

    public CausalityClock getPruneClock() {
        return pruneClock;
    }
}
