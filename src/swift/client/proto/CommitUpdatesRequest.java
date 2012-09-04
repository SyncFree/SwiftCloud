package swift.client.proto;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

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
public class CommitUpdatesRequest extends ClientRequest {
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    protected Timestamp baseTimestamp;
    // Optimization HACK! dependencyClock is stored together for transfer time
    // only. We should have a cleaner way to do it, perhaps with Kryo
    // serializer?
    protected CausalityClock dependencyClock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitUpdatesRequest() {
    }

    public CommitUpdatesRequest(String clientId, final Timestamp baseTimestamp,
            List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
        super(clientId);
        this.baseTimestamp = baseTimestamp;
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups.size());
        // Part of optimization hack.
        for (final CRDTObjectUpdatesGroup<?> ops : objectUpdateGroups) {
            this.dependencyClock = ops.getDependency();
            this.objectUpdateGroups.add(ops.withBaseTimestampAndDependency(baseTimestamp, null));
        }
    }

    /**
     * @return valid base timestamp for all updates in the request, previously
     *         obtained using {@link GenerateTimestampRequest}; all individual
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
    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        // // Part of optimization hack: make sure dependencies are filled in.
        for (final CRDTObjectUpdatesGroup<?> ops : objectUpdateGroups) {
            ops.setDependency(dependencyClock);
        }
        return objectUpdateGroups;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
