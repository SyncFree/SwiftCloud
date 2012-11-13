package swift.clocks;

/**
 * TimestampSource decorator that allows to explicitly return and later reuse
 * last generated timestamp. Useful for generating continuous sequences for
 * CausalityClock when some timestamps are not included in the CausalityClock -
 * they can be returned to the generator instead.
 * 
 * @author mzawirski
 */
public class ReturnableTimestampSourceDecorator<T extends Timestamp> implements TimestampSource<T> {
    private final TimestampSource<T> origSource;
    private boolean lastTimestampReturned;
    private T lastTimestamp;

    public ReturnableTimestampSourceDecorator(TimestampSource<T> origSource) {
        this.origSource = origSource;
    }

    @Override
    public T generateNew() {
        if (lastTimestampReturned) {
            lastTimestampReturned = false;
            return lastTimestamp;
        }
        lastTimestamp = origSource.generateNew();
        return lastTimestamp;
    }

    public void returnLastTimestamp() {
        if (lastTimestamp != null) {
            lastTimestampReturned = true;
        }
    }
}
