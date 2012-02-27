package swift.clocks;

import swift.crdt.interfaces.TimestampSource;

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

    public IncrementalTripleTimestampGenerator(Timestamp ts) {
    	this.siteid = ts.getIdentifier();
    	this.counter = ts.getCounter();
    	this.last = Timestamp.MIN_VALUE;
    	
    }

    @Override
    public synchronized TripleTimestamp generateNew() {
        return new TripleTimestamp( siteid, counter, ++last);
    }

}
