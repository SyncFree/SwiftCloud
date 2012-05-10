package swift.dc.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
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
    protected CausalityClock dcClock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public SeqCommitUpdatesReply() {
    }

    public SeqCommitUpdatesReply(CausalityClock dcClock) {
        this.dcClock = dcClock;
    }

    /**
     * @return commit status
     */
    public CausalityClock getDCClock() {
        return dcClock;
    }


    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SeqCommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }
}
