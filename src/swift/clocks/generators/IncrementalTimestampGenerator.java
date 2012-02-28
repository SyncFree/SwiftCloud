package swift.clocks.generators;

import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;


/**
 * Timestamp generator for a given site. Always generates the consecutive
 * counter for the given site.
 * 
 * @author nmp
 * 
 */
public class IncrementalTimestampGenerator implements TimestampSource<Timestamp> {

    private String siteid;
    private long last;

    public IncrementalTimestampGenerator(String siteid) {
        this(siteid, 0);
    }

    public IncrementalTimestampGenerator(String siteid, long last) {
        this.siteid = siteid;
        this.last = last;
    }

    @Override
    public synchronized Timestamp generateNew() {
        return new Timestamp(siteid, ++last);
    }

}
