package swift.client.proto;

import java.util.ArrayList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to commit set of updates to the store.
 * <p>
 * All updates use the same client timestamp. Updates are organized into atomic
 * groups of updates per each object.
 * 
 * @author mzawirski
 */
// TODO: We can consider more space-efficient representation of this message
// that require a bit of extra processing at the server (e.g. baseTimestamp and
// dependency CausalityClock is shared by all updates).
public class CommitUpdatesRequest extends ClientRequest {
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    // Optimization HACK! dependencyClock and timestampMapping are unified to
    // limit data structure size and message size.
    // We should have a cleaner way to do it, perhaps with Kryo serializer?
    // Shall we really share these instances?
    protected TimestampMapping timestampMapping;
    protected CausalityClock dependencyClock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitUpdatesRequest() {
    }

    public CommitUpdatesRequest(String clientId, final Timestamp baseTimestamp,
            List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
        super(clientId);
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups.size());
        // Part of optimization hack.
        for (final CRDTObjectUpdatesGroup<?> ops : objectUpdateGroups) {
            if (this.dependencyClock == null) {
                this.dependencyClock = ops.getDependency();
                this.timestampMapping = ops.getTimestampMapping();
            }
            ops.init(this.timestampMapping, this.dependencyClock);
            this.objectUpdateGroups.add(ops);
        }
    }

    /**
     * @return valid base timestamp for all updates in the request, previously
     *         obtained using {@link GenerateTimestampRequest}; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getBaseTimestamp() {
        return timestampMapping.getClientTimestamp();
    }

    /**
     * @return list of groups of object operations; there is at most one group
     *         per object and they all share the same base timestamp
     *         {@link #getBaseTimestamp()}
     */
    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
