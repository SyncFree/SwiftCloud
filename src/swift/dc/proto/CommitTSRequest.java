package swift.dc.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
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
    protected Timestamp cltTimestamp;
    protected CausalityClock version;   // observed version
    protected boolean commit;           // true if transaction was committed
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    
    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitTSRequest() {
    }

    public CommitTSRequest(Timestamp timestamp, Timestamp cltTimestamp, CausalityClock version, boolean commit,
            List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.version = version;
        this.commit = commit;
        this.objectUpdateGroups = objectUpdateGroups;
    }


    /**
     * @return the timestamp previously received from the server
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return the timestamp of the client
     */
    public Timestamp getCltTimestamp() {
        return cltTimestamp;
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

    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

}
