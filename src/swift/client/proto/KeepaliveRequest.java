package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

public class KeepaliveRequest {
    protected Timestamp timestamp;
    protected CausalityClock version;
}
