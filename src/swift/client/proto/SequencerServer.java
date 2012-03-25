package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * Server interface for client-server sequencer interaction.
 * <p>
 * This interface defines interaction between clients and the sequencer server.
 */
public interface  SequencerServer extends RpcHandler {
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
     *            connection such that the remote end implements
     *            {@link LatestKnownClockReplyHandler} and expects
     *            {@link LatestKnownClockReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, LatestKnownClockRequest request);
    

}
