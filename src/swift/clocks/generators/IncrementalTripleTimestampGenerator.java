package swift.clocks.generators;

import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;
import swift.clocks.TripleTimestamp;
import swift.exceptions.InvalidParameterException;

/**
 * TripleTimestamp generator based on an existing Timestamp.
 * 
 * @author nmp
 * 
 */
public class IncrementalTripleTimestampGenerator implements TimestampSource<TripleTimestamp> {

    private String siteid;
    private long counter;
    private long last;

    public IncrementalTripleTimestampGenerator(Timestamp ts) throws InvalidParameterException {
        this.siteid = ts.getIdentifier();
        if( siteid == null) {
            throw new InvalidParameterException();
        }
        this.counter = ts.getCounter();
        this.last = Timestamp.MIN_VALUE;

    }

    @Override
    public synchronized TripleTimestamp generateNew() {
        return new TripleTimestamp(siteid, counter, ++last);
    }

}
