package swift.client.proto;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
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
        /**
         * The transaction has been committed using a known timestamp or
         * timestamps given in the reply.
         */
        COMMITTED_WITH_KNOWN_TIMESTAMPS,
        /**
         * The transaction has been committed using an unknown timestamp, which
         * is included somewhere in the clock given the reply.
         */
        COMMITTED_WITH_KNOWN_CLOCK_RANGE,
        /**
         * The transaction cannot be committed, because a given operation is
         * invalid for some reason.
         */
        INVALID_OPERATION
    }

    protected CommitStatus status;
    protected List<Timestamp> systemTimestamps;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitUpdatesReply() {
    }

    public CommitUpdatesReply(CommitStatus status, List<Timestamp> systemTimestamps) {
        this.status = status;
        this.systemTimestamps = new LinkedList<Timestamp>(systemTimestamps);
    }

    public CommitUpdatesReply(CommitStatus status, Timestamp systemTimestamp) {
        this.status = status;
        this.systemTimestamps = new LinkedList<Timestamp>(systemTimestamps);
        this.systemTimestamps.add(systemTimestamp);
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
    public List<Timestamp> getCommitTimestamps() {
        return Collections.unmodifiableList(systemTimestamps);
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((CommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
