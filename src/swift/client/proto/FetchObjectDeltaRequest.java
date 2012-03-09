package swift.client.proto;

import swift.clocks.CausalityClock;

public class FetchObjectDeltaRequest extends FetchObjectVersionRequest {
    protected CausalityClock knownVersion;
}
