package swift.clocks;


/**
 * Timestamp generator for a given site. Always generates the max from the
 * consecutive counter for the given site and a given causality clock.
 * 
 * NOTE: if the given clock is updated outside of this class, the next clock
 * will take this into consideration.
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
        if (siteid == null) {
            throw new NullPointerException();
        }
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
