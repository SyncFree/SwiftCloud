package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server confirmation of committed updates.
 * 
 * @author mzawirski
 * @see CommitUpdatesRequest
 */
public class CommitUpdatesReply implements RpcMessage {
    public enum CommitStatus {
        // TODO: discuss fault-tolerance, this is not really trivial if we do
        // not want to pay high price for the implementation
        COMMITTED,
        /**
         * The transaction has been already committed using another timestamp.
         * TODO specify this timestamp?
         */
        ALREADY_COMMITTED,
        /**
         * The transaction cannot be committed, because provided timestamp is
         * invalid. Client can retry to commit using another timestamp.
         */
        INVALID_TIMESTAMP
    }

    protected CommitStatus status;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CommitUpdatesReply() {
    }

    public CommitUpdatesReply(CommitStatus status) {
        super();
        this.status = status;
    }

    /**
     * @return commit status
     */
    public CommitStatus getStatus() {
        return status;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((CommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
