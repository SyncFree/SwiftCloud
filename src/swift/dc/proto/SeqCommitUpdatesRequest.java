package swift.dc.proto;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import swift.client.proto.*;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Message with committted transaction.
 * <p>
 * All updates use the same timestamp. Updates are organized into atomic groups
 * of updates per each object.
 * 
 * @author preguica
 */
public class SeqCommitUpdatesRequest implements RpcMessage {
    protected String dcName;
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    protected Timestamp timestamp;
    protected Timestamp cltTimestamp;
    CausalityClock dcReceived;
    CausalityClock dcNotUsed;
    public transient long lastSent;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    SeqCommitUpdatesRequest() {
    }

    public SeqCommitUpdatesRequest(String dcName, final Timestamp timestamp, final Timestamp cltTimestamp,
            List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, CausalityClock dcReceived, CausalityClock dcNotUsed) {
        this.dcName = dcName;
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups);
        this.dcReceived = dcReceived;
        this.dcNotUsed = dcNotUsed;
    }

    /**
     * @return valid base timestamp for all updates in the request, previously
     *         obtained using {@link GenerateTimestampRequest}; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return client timestamp for this transaction; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getCltTimestamp() {
        return cltTimestamp;
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
        ((BaseServer) handler).onReceive(conn, this);
    }

    public CausalityClock getDcReceived() {
        return dcReceived;
    }

    public CausalityClock getDcNotUsed() {
        return dcNotUsed;
    }

    public String getDcName() {
        return dcName;
    }
}
