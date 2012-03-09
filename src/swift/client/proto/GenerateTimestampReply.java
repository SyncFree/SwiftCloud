package swift.client.proto;

import swift.clocks.Timestamp;

public class GenerateTimestampReply {
    protected Timestamp timestamp;
    /**
     * When the timestamp becomes invalid (approximate). Milliseconds since the
     * UNIX epoch.
     */
    protected long validityMillis;
}
