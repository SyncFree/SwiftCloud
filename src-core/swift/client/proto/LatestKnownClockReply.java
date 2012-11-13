package swift.client.proto;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to client's latest known clock request.
 * 
 * @author mzawirski
 */
public class LatestKnownClockReply implements RpcMessage {
    private CausalityClock clock;
    private CausalityClock disasterDurableClock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    LatestKnownClockReply() {
    }

    public LatestKnownClockReply(final CausalityClock clock, final CausalityClock disasterDurableClock) {
        this.clock = clock;
        this.disasterDurableClock = disasterDurableClock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate
     */
    public CausalityClock getClock() {
        return clock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate, durable in even in case of disaster affecting fragment
     *         of the store
     */
    public CausalityClock getDistasterDurableClock() {
        return disasterDurableClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((LatestKnownClockReplyHandler) handler).onReceive(conn, this);
    }
}
