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
    public enum GenerateStatus {
        /**
         * Timestamp generated
         */
        SUCCESSS,
        /**
         * Already committed.
         */
        ALREADY_COMMITTED,
        /**
         * The transaction cannot be committed, because a given operation is
         * invalid for some reason.
         */
        INVALID_OPERATION
    }

    protected GenerateStatus status;
    protected Timestamp timestamp;
    protected long cltClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateDCTimestampReply() {
    }

    public GenerateDCTimestampReply(final Timestamp timestamp, final long cltClock) {
        this.status = GenerateStatus.SUCCESSS;
        this.timestamp = timestamp;
        this.cltClock = cltClock;
    }

    public GenerateDCTimestampReply(final long cltClock) {
        this.status = GenerateStatus.ALREADY_COMMITTED;
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
        ((GenerateDCTimestampReplyHandler) handler).onReceive(conn, this);
    }

    public GenerateStatus getStatus() {
        return status;
    }

    public long getCltClock() {
        return cltClock;
    }
}
