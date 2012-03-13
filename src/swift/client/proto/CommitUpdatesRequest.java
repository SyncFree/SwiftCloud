package swift.client.proto;

import java.util.List;

import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectOperationsGroup;

/**
 * Client request to commit set of updates to the store.
 * <p>
 * All updates use the same timestamp. Updates are organized into atomic groups
 * of updates per each object.
 * 
 * @author mzawirski
 */
// TODO: We can consider more space-efficient representation of this message
// that require a bit of extra processing at the server (e.g. baseTimestamp and
// dependency CausalityClock is shared by all updates).
public class CommitUpdatesRequest {
    protected List<CRDTObjectOperationsGroup> objectUpdateGroups;
    protected Timestamp baseTimestamp;

    /**
     * Constructor for Kryo serialization.
     */
    public CommitUpdatesRequest() {
    }

    public CommitUpdatesRequest(final Timestamp baseTimestamp, List<CRDTObjectOperationsGroup> objectUpdateGroups) {
        this.baseTimestamp = baseTimestamp;
        this.objectUpdateGroups = objectUpdateGroups;
    }

    /**
     * @return base timestamp for all updates in the request; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getBaseTimestamp() {
        return baseTimestamp;
    }

    /**
     * @return list of groups of object operations; there is at most one group
     *         per object and they all share the same base timestamp
     *         {@link #getBaseTimestamp()}
     */
    public List<CRDTObjectOperationsGroup> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }
}
