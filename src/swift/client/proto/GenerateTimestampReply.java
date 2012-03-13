package swift.client.proto;

import swift.clocks.Timestamp;

/**
 * Timestamp given by the server to the client.
 * 
 * @author mzawirski
 */
public class GenerateTimestampReply {
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
     *         keepalive; specified in milliseconds since the UNIX epoch
     */
    public long getValidityMillis() {
        return validityMillis;
    }
}
