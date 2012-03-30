package swift.dc.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcConnection;
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

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CommitTSRequest() {
    }

    public CommitTSRequest(final Timestamp timestamp, final CausalityClock version, final boolean commit) {
        this.timestamp = timestamp;
        this.version = version;
        this.commit = commit;
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
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SequencerServer) handler).onReceive(conn, this);
    }
}
