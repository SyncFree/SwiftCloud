package swift.clocks.generators;

import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;
import swift.exceptions.InvalidParameterException;


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

    public IncrementalTimestampGenerator(String siteid) throws InvalidParameterException {
        this(siteid, 0);
    }

    public IncrementalTimestampGenerator(String siteid, long last) throws InvalidParameterException {
        if( siteid == null) {
            throw new InvalidParameterException();
        }
        this.siteid = siteid;
        this.last = last;
    }

    @Override
    public synchronized Timestamp generateNew() {
        return new Timestamp(siteid, ++last);
    }

}
