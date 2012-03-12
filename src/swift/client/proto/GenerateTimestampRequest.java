package swift.client.proto;

import swift.clocks.CausalityClock;

public class GenerateTimestampRequest {
    protected CausalityClock dominatedClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    public GenerateTimestampRequest() {
    }

    public GenerateTimestampRequest(CausalityClock dominatedClock) {
        this.dominatedClock = dominatedClock;
    }

    public CausalityClock getDominatedClock() {
        return dominatedClock;
    }
}
