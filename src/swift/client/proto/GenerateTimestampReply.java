package swift.client.proto;

import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Timestamp given by the server to the client.
 * 
 * @author mzawirski
 */
public class GenerateTimestampReply implements RpcMessage {
    protected Timestamp timestamp;
    protected long validityMillis;

    // Fake constructor for Kryo serialization. Do NOT use.
    public GenerateTimestampReply() {
    }

    public GenerateTimestampReply(final Timestamp timestamp, final long validityMillis) {
        this.timestamp = timestamp;
        this.validityMillis = validityMillis;
    }

    /**
     * @return timestamp that client can use, subject to renewal using keepalive
     *         message
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return until what time the timestamp stays valid unless extended using
     *         keepalive; specified in milliseconds since the UNIX epoch; after
     *         that time client committing updates using this timestamp might be
     *         rejected to commit
     */
    public long getValidityMillis() {
        return validityMillis;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((GenerateTimestampReplyHandler) handler).onReceive(conn, this);
    }
}
