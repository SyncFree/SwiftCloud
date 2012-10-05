package swift.dc.proto;

import swift.client.proto.GenerateTimestampReplyHandler;
import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Timestamp given by the server to the client.
 * 
 * @author nmp
 */
public class GenerateDCTimestampReply implements RpcMessage {
    protected boolean valid;
    protected Timestamp timestamp;
    protected long cltClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateDCTimestampReply() {
    }

    public GenerateDCTimestampReply(final Timestamp timestamp, final long cltClock) {
        this.valid = true;
        this.timestamp = timestamp;
        this.cltClock = cltClock;
    }

    public GenerateDCTimestampReply(final long cltClock) {
        this.valid = false;
        this.cltClock = cltClock;
    }

    /**
     * @return timestamp that client can use, subject to renewal using keepalive
     *         message
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((GenerateTimestampReplyHandler) handler).onReceive(conn, this);
    }

    public boolean isValid() {
        return valid;
    }

    public long getCltClock() {
        return cltClock;
    }
}
