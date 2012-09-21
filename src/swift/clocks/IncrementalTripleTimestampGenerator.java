package swift.clocks;

/**
 * TripleTimestamp generator based on an existing Timestamp.
 * 
 * @author nmp
 * 
 */
public class IncrementalTripleTimestampGenerator implements TimestampSource<TripleTimestamp> {

    private final Timestamp ts;
    private long last;

    public IncrementalTripleTimestampGenerator(Timestamp ts) {
        this.ts = ts;
        this.last = Timestamp.MIN_VALUE;

    }

    @Override
    public synchronized TripleTimestamp generateNew() {
        return new TripleTimestamp(ts, ++last);
    }

}
