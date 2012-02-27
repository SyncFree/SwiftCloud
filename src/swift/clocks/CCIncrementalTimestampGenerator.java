package swift.clocks;

import swift.crdt.interfaces.TimestampSource;
import swift.exceptions.InvalidParameterException;

/**
 * Timestamp generator that acts only on a given causality clock.
 * 
 * @author nmp
 * 
 */
public class CCIncrementalTimestampGenerator implements
        TimestampSource<Timestamp> {
    private String siteid;

    protected CCIncrementalTimestampGenerator(String siteid) {
        this.siteid = siteid;
    }

    @Override
    public synchronized Timestamp generateNew()
            throws InvalidParameterException {
        throw new InvalidParameterException();
    }

    @Override
    public <V extends CausalityClock<V>> Timestamp generateNew(
            CausalityClock<V> c) {
        long next = c.getLatestCounter(siteid);
        Timestamp ts = new Timestamp(siteid, ++next);
        c.record(ts);
        return ts;
    }

}
