package swift.dc.proto;

import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import sys.net.api.rpc.RpcConnection;
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

    public CommitTSReply() {
    }

    public CommitTSReply(CommitTSStatus status) {
        this.status = status;
    }

    /**
     * @return status code of the reply
     */
    public CommitTSStatus getStatus() {
        return status;
    }


    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((CommitTSReplyHandler) handler).onReceive(conn, this);
    }
}
