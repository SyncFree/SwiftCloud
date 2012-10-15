package swift.client.proto;

import java.util.ArrayList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to commit set of updates to the store.
 * <p>
 * All updates use the same client timestamp and will receive a system
 * timestamp(s) during commit. Updates are organized into atomic groups of
 * updates per each object.
 * 
 * @author mzawirski
 */
public class CommitUpdatesRequest extends ClientRequest {
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    protected Timestamp clientTimestamp;
    protected CausalityClock dependencyClock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitUpdatesRequest() {
    }

    public CommitUpdatesRequest(String clientId, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
        super(clientId);
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups);
        this.clientTimestamp = this.objectUpdateGroups.get(0).getClientTimestamp();
        this.dependencyClock = this.objectUpdateGroups.get(0).getDependency();
    }

    /**
     * @return valid base timestamp for all updates in the request, previously
     *         obtained using {@link GenerateTimestampRequest}; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getClientTimestamp() {
        return clientTimestamp;
    }

    /**
     * @return list of groups of object operations; there is at most one group
     *         per object; note that all groups share the same base client
     *         timestamp ( {@link #getClientTimestamp()}), timestamp mappings
     *         and dependency clock.
     * 
     */
    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }

    public void addTimestampsToDeps(List<Timestamp> tsLst) {
        if (tsLst != null) {
            for (Timestamp t : tsLst) {
                this.dependencyClock.record(t);
            }
        }
    }
}
