package swift.dc.proto;

import swift.client.proto.BaseServer;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampReplyHandler;
import sys.net.api.rpc.RpcHandle;
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
    void onReceive(RpcHandle conn, CommitTSRequest request);
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link GenerateTimestampReplyHandler} and expects
     *            {@link GenerateTimestampReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, GenerateDCTimestampRequest request);


}
