package swift.dc.proto;

import swift.client.proto.BaseServer;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;

/**
 * Server interface for client-server sequencer interaction.
 * <p>
 * This interface defines interaction between clients and the sequencer server.
 */
public interface  SequencerServer extends BaseServer {
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link CommitTSReplyHandler} and expects
     *            {@link CommitTSReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcConnection conn, CommitTSRequest request);

}
