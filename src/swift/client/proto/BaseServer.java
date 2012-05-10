package swift.client.proto;

import swift.dc.proto.SeqCommitUpdatesReply;
import swift.dc.proto.SeqCommitUpdatesReplyHandler;
import swift.dc.proto.SeqCommitUpdatesRequest;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Server interface for client-server interaction.
 * <p>
 * This interface defines the common functions of the surrogate
 * and sequencer.
 */
public interface  BaseServer extends RpcHandler {
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link GenerateTimestampReplyHandler} and expects
     *            {@link GenerateTimestampReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, GenerateTimestampRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link KeepaliveReplyHandler} and expects
     *            {@link KeepaliveReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, KeepaliveRequest request);
    
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link LatestKnownClockReplyHandler} and expects
     *            {@link LatestKnownClockReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, LatestKnownClockRequest request);
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link SeqCommitUpdatesReplyHandler} and expects
     *            {@link SeqCommitUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, SeqCommitUpdatesRequest request);

    

}
