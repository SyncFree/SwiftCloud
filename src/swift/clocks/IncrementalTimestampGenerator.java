package swift.clocks;

import swift.crdt.interfaces.TimestampSource;

/**
 * Timestamp generator for a given site. Always generates the consecutive
 * counter for the given site.
 * 
 * @author nmp
 * 
 */
public class IncrementalTimestampGenerator implements
        TimestampSource<Timestamp> {

    private String siteid;
    private long last;

    protected IncrementalTimestampGenerator(String siteid) {
        this(siteid, 0);
    }

    protected IncrementalTimestampGenerator(String siteid, long last) {
        this.siteid = siteid;
        this.last = last;
    }

    @Override
    public synchronized Timestamp generateNew() {
        return new Timestamp(siteid, ++last);
    }

    @Override
    public <V extends CausalityClock<V>> Timestamp generateNew(
            CausalityClock<V> c) {
        last = Math.max(last, c.getLatestCounter(siteid));
        return new Timestamp(siteid, ++last);
    }

}
