package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

public class KeepaliveRequest {
    private Timestamp timestamp;
    private CausalityClock version;
}
