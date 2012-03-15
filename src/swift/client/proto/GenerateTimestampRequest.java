package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
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
    protected Timestamp previousTimestamp;

    // Fake constructor for Kryo serialization. Do NOT use.
    public GenerateTimestampRequest() {
    }

    public GenerateTimestampRequest(CausalityClock dominatedClock, Timestamp previousTimestamp) {
        this.dominatedClock = dominatedClock;
        this.previousTimestamp = previousTimestamp;
    }

    /**
     * @return the clock that the requested timestamp should dominate (to
     *         enforce invariant that later timestamp is never dominated by
     *         earlier)
     */
    public CausalityClock getDominatedClock() {
        return dominatedClock;
    }

    /**
     * @return optional previous timestamp acquired from some server, invalid /
     *         rejected; can be null
     */
    public Timestamp getPreviousTimestamp() {
        return previousTimestamp;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
