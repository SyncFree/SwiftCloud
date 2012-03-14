package swift.client.proto;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Client request to generate a timestamp for a transaction.
 * 
 * @author mzawirski
 */
public class GenerateTimestampRequest implements RpcMessage {
    protected CausalityClock dominatedClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    public GenerateTimestampRequest() {
    }

    public GenerateTimestampRequest(CausalityClock dominatedClock) {
        this.dominatedClock = dominatedClock;
    }

    /**
     * @return the clock that the requested timestamp should dominate (to
     *         enforce invariant that later timestamp is never dominated by
     *         earlier)
     */
    public CausalityClock getDominatedClock() {
        return dominatedClock;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
