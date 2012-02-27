package swift.clocks;

import swift.crdt.interfaces.TimestampSource;

/**
 * Timestamp generator for a given site. Always generates the consecutive
 * counter for the given site.
 * 
 * @author nmp
 * 
 */
public class CCIncrementalTimestampGenerator implements TimestampSource<Timestamp> {

    private String siteid;
    private CausalityClock clock;
    private long last;

    public CCIncrementalTimestampGenerator(String siteid, CausalityClock clock) {
        this(siteid, clock, Timestamp.MIN_VALUE);
    }

    public CCIncrementalTimestampGenerator(String siteid, CausalityClock clock, long last) {
        this.siteid = siteid;
        this.clock = clock;
        this.last = last;
    }

    @Override
    public synchronized Timestamp generateNew() {
        last = Math.max(last, clock.getLatestCounter(siteid));
        return new Timestamp(siteid, ++last);
    }

}
