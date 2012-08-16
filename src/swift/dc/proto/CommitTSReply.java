package swift.dc.proto;

import java.util.List;

import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to client committs message.
 * 
 * @author preguica
 */
public class CommitTSReply implements RpcMessage {
    public enum CommitTSStatus {
        /**
         * The reply contains requested version.
         */
        OK,
        /**
         * The requested object is not in the store.
         */
        FAILED
    }

    protected CommitTSStatus status;
    protected CausalityClock currVersion;


    public CommitTSReply() {
    }

    public CommitTSReply(CommitTSStatus status, CausalityClock currVersion) {
        super();
        this.status = status;
        this.currVersion = currVersion;
    }


    /**
     * @return status code of the reply
     */
    public CommitTSStatus getStatus() {
        return status;
    }

    /**
     * @return the current version in the server
     */
    public CausalityClock getCurrVersion() {
        return currVersion;
    }


    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((CommitTSReplyHandler) handler).onReceive(conn, this);
    }
}
