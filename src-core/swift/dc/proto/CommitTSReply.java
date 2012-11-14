package swift.dc.proto;

import swift.clocks.CausalityClock;
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
    protected CausalityClock stableVersion;


    public CommitTSReply() {
    }

    public CommitTSReply(CommitTSStatus status,  CausalityClock currVersion, CausalityClock stableVersion) {
        super();
        this.status = status;
        this.currVersion = currVersion;
        this.stableVersion = stableVersion;
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

    /**
     * @return the current version in the server
     */
    public CausalityClock getStableVersion() {
        return stableVersion;
    }


    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((CommitTSReplyHandler) handler).onReceive(conn, this);
    }
}