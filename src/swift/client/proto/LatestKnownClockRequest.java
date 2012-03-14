package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Client request to get the latest known clock at the server.
 * 
 * @author mzawirski
 */
public class LatestKnownClockRequest implements RpcMessage {

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
