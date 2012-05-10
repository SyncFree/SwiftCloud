package swift.dc.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Informs the Sequencer Server that the given timestamp should be
 * committed/rollbacked.
 * 
 * @author preguica
 */
public class CommitTSRequest implements RpcMessage {
    protected Timestamp timestamp;
    protected CausalityClock version;   // observed version
    protected boolean commit;           // true if transaction was committed
    protected List<CRDTObjectOperationsGroup<?>> objectUpdateGroups;
    
    protected Timestamp baseTimestamp;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CommitTSRequest() {
    }

    public CommitTSRequest(Timestamp timestamp, CausalityClock version, boolean commit,
            List<CRDTObjectOperationsGroup<?>> objectUpdateGroups, Timestamp baseTimestamp) {
        this.timestamp = timestamp;
        this.version = version;
        this.commit = commit;
        this.objectUpdateGroups = objectUpdateGroups;
        this.baseTimestamp = baseTimestamp;
    }


    /**
     * @return the timestamp previously received from the server
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return the oldest snapshot in use by the client
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return true if this is called in a commit
     */
    public boolean getCommit() {
        return commit;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SequencerServer) handler).onReceive(conn, this);
    }

    public List<CRDTObjectOperationsGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

    public Timestamp getBaseTimestamp() {
        return baseTimestamp;
    }
}
