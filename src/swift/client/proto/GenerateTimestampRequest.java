package swift.client.proto;

import swift.clocks.CausalityClock;

/**
 * Client request to generate a timestamp for a transaction.
 * 
 * @author mzawirski
 */
public class GenerateTimestampRequest {
    protected CausalityClock dominatedClock;

    // Fake constructor for Kryo serialization. Do NOT use.
    public GenerateTimestampRequest() {
    }

    public GenerateTimestampRequest(CausalityClock dominatedClock) {
        this.dominatedClock = dominatedClock;
    }

    /**
     * @return the clock that the requested timestamp should dominate (to
     *         enforce invariant that later timestamp is never dominated by
     *         earlier)
     */
    public CausalityClock getDominatedClock() {
        return dominatedClock;
    }
}
