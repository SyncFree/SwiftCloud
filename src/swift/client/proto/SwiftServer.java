package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * Server interface for client-server interaction.
 * <p>
 * This interface defines interaction between clients and a single server, i.e.
 * sessions with affinity. Server can identify originating client using
 * Catadupa/RPC primitives available in messages. For documentation of
 * particular requests, see message definitions.
 * 
 * @author mzawirski
 */
public interface SwiftServer extends RpcHandler {
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link FetchObjectVersionReplyHandler} and expects
     *            {@link FetchObjectVersionReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, FetchObjectVersionRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link FetchObjectVersionReplyHandler} and expects
     *            {@link FetchObjectVersionReply}
     * @param request
     *            request to serve
     */
    // TODO: FetchObjectVersionReply is temporary. Eventually, we shall replace
    // it with deltas or list of operations.
    void onReceive(RpcConnection conn, FetchObjectDeltaRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link GenerateTimestampReplyHandler} and expects
     *            {@link GenerateTimestampReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, GenerateTimestampRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link KeepaliveReplyHandler} and expects
     *            {@link KeepaliveReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, KeepaliveRequest request);

    /**
     * @param conn
     *            connection that does not expect any message
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, UnsubscribeNotificationsRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link CommitUpdatesReplyHandler} and expects
     *            {@link CommitUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, CommitUpdatesRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link LatestKnownClockReplyHandler} and expects
     *            {@link LatestKnownClockReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, LatestKnownClockRequest request);
}
