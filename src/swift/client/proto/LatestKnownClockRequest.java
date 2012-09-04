package swift.client.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to get the latest known clock at the server.
 * 
 * @author mzawirski
 */
public class LatestKnownClockRequest extends ClientRequest {

    /**
     * Constructor for Kryo serialization.
     */
    LatestKnownClockRequest() {
    }

    public LatestKnownClockRequest(String clientId) {
        super(clientId);
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((BaseServer) handler).onReceive(conn, this);
    }
}
