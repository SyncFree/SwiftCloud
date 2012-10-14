package swift.client.proto;

import java.util.LinkedList;
import java.util.List;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server confirmation for a batch of committed updates, with information on
 * status and used timestamp.
 * 
 * @author mzawirski
 * @see BatchCommitUpdatesRequest
 * @see CommitUpdatesReply
 */
public class BatchCommitUpdatesReply implements RpcMessage {
    protected List<CommitUpdatesReply> replies;

    /**
     * FAKE CONSTRUCTOR ONLY FOR Kryo!
     */
    public BatchCommitUpdatesReply() {
    }

    /**
     * @param replies
     *            commit updates replies, in order as they appear in the
     *            original {@link BatchCommitUpdatesRequest}
     */
    public BatchCommitUpdatesReply(List<CommitUpdatesReply> replies) {
        this.replies = new LinkedList<CommitUpdatesReply>(replies);
    }

    /**
     * @return commit replies, mutable; in order as they appear in the original
     *         {@link BatchCommitUpdatesRequest}
     */
    public List<CommitUpdatesReply> getReplies() {
        return replies;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((BatchCommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
