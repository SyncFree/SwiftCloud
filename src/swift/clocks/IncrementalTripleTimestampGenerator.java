package swift.clocks;

/**
 * TripleTimestamp generator based on an existing Timestamp.
 * 
 * @author nmp
 * 
 */
public class IncrementalTripleTimestampGenerator implements TimestampSource<TripleTimestamp> {

    private final TimestampMapping mapping;
    private long last;

    public IncrementalTripleTimestampGenerator(TimestampMapping mapping) {
        this.mapping = mapping;
        this.last = Timestamp.MIN_VALUE;

    }

    @Override
    public synchronized TripleTimestamp generateNew() {
        return new TripleTimestamp(mapping, ++last);
    }

}
