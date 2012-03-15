package swift.client.proto;

import swift.clocks.Timestamp;
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
        COMMITTED,
        /**
         * The transaction has been already committed using another timestamp.
         */
        ALREADY_COMMITTED,
        /**
         * The transaction cannot be committed, because provided timestamp is
         * invalid. Client can retry to commit using another timestamp.
         */
        INVALID_TIMESTAMP
    }

    protected CommitStatus status;
    protected Timestamp commitTimestamp;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public CommitUpdatesReply() {
    }

    public CommitUpdatesReply(CommitStatus status, Timestamp commitTimestamp) {
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }

    /**
     * @return commit status
     */
    public CommitStatus getStatus() {
        return status;
    }

    /**
     * @return timestamp using which the transaction has been committed; null if
     *         {@link #getStatus()} is {@link CommitStatus#INVALID_TIMESTAMP}
     */
    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((CommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
