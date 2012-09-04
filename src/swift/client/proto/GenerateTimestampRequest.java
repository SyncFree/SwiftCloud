package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to generate a timestamp for a transaction.
 * 
 * @author mzawirski
 */
public class GenerateTimestampRequest extends ClientRequest {
    protected CausalityClock dominatedClock;
    protected Timestamp previousTimestamp;

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateTimestampRequest() {
    }

    public GenerateTimestampRequest(String clientId, CausalityClock dominatedClock, Timestamp previousTimestamp) {
        super(clientId);
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
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((BaseServer) handler).onReceive(conn, this);
    }
}
