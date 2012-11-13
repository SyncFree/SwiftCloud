package swift.dc.proto;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server confirmation of committed updates.
 * 
 * @author preguica
 * @see SeqCommitUpdatesRequest
 */
public class SeqCommitUpdatesReply implements RpcMessage {
    protected String dcName;
    protected CausalityClock dcClock;  // applied operations at given data center
    protected CausalityClock dcStableClock;  // stable operations at given data center
    protected CausalityClock dcKnownClock;  // known operations at given data center

    /**
     * Fake constructor for Kryo serialization. Do NOT use. 
     * REMARK: smd, however it is being used by the sequencer????
     */
    public SeqCommitUpdatesReply() {
    }

    public SeqCommitUpdatesReply(String dcName, CausalityClock dcClock, CausalityClock stableClock, CausalityClock knownClock) {
        this.dcName = dcName;
        this.dcClock = dcClock;
        this.dcStableClock = stableClock;
        this.dcKnownClock = knownClock;
    }

    /**
     * @return DC clock
     */
    public CausalityClock getDCClock() {
        return dcClock;
    }


    /**
     * @return stable clock
     */
    public CausalityClock getDCStableClock() {
        return dcStableClock;
    }


    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SeqCommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }

    public CausalityClock getDcKnownClock() {
        return dcKnownClock;
    }

    public String getDcName() {
        return dcName;
    }
}
