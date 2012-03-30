package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
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
    public LatestKnownClockRequest() {
    }

    public LatestKnownClockRequest(String clientId) {
        super(clientId);
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((BaseServer) handler).onReceive(conn, this);
    }
}
