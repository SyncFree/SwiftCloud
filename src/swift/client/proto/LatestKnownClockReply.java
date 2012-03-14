package swift.client.proto;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to client's latest known clock request.
 * 
 * @author mzawirski
 */
public class LatestKnownClockReply implements RpcMessage {
    private CausalityClock clock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public LatestKnownClockReply() {
    }

    public LatestKnownClockReply(final CausalityClock clock) {
        this.clock = clock;
    }

    /**
     * @return latest known clock in the system, i.e. valid snapshot point
     *         candidate
     */
    public CausalityClock getClock() {
        return clock;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((LatestKnownClockReplyHandler) handler).onReceive(conn, this);
    }
}
