package swift.client.proto;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server confirmation of committed updates, with information on used timestamp.
 * 
 * @author mzawirski
 * @see CommitUpdatesRequest
 */
public class CommitUpdatesReply implements RpcMessage {
    public enum CommitStatus {
        /**
         * The transaction has been committed using known timestamp or
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
    protected List<Timestamp> commitTimestamps;
    protected CausalityClock commitClock;

    /**
     * Create a reply with known commit system timestamps.
     * 
     * @param systemTimestamps
     */
    public CommitUpdatesReply(Timestamp... systemTimestamps) {
        this.status = CommitStatus.COMMITTED_WITH_KNOWN_TIMESTAMPS;
        this.commitTimestamps = new LinkedList<Timestamp>(Arrays.asList(systemTimestamps));
    }

    /**
     * Create a reply with known imprecise commitClock.
     * 
     * @param commitClock
     */
    public CommitUpdatesReply(CausalityClock commitClock) {
        this.status = CommitStatus.COMMITTED_WITH_KNOWN_CLOCK_RANGE;
        this.commitClock = commitClock;
    }

    /**
     * Create a reply with invalid status..
     */
    public CommitUpdatesReply() {
        this.status = CommitStatus.INVALID_OPERATION;
    }

    /**
     * @return commit status
     */
    public CommitStatus getStatus() {
        return status;
    }

    /**
     * @return when status is
     *         {@link CommitStatus#COMMITTED_WITH_KNOWN_TIMESTAMPS}, a list of
     *         system timestamps that are known at DC to represent the commit of
     *         the transaction; otherwise null
     */
    public List<Timestamp> getCommitTimestamps() {
        return Collections.unmodifiableList(commitTimestamps);
    }

    /**
     * @return when status is
     *         {@link CommitStatus#COMMITTED_WITH_KNOWN_CLOCK_RANGE}, an
     *         imprecise clock including transaction commit timestamp; otherwise
     *         null
     */
    public CausalityClock getImpreciseCommitClock() {
        return commitClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((CommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
