package swift.clocks;

/**
 * Timestamp implementation with two-dimensional counter (which accounts for
 * triple together with siteId).
 */
public class TripleTimestamp extends Timestamp {
    public static final long INIT_PRIMARY_VALUE = Long.MAX_VALUE;
    private static final long serialVersionUID = 1L;
    protected final long secondaryCounter;

    /**
     * Creates triple counter with default maximum value for primary counter.
     * 
     * @param siteId
     * @param primaryCounter
     * @param secondaryCounter
     */
//    public TripleTimestamp(final String siteId, final long secondaryCounter) {
//        this(siteId, INIT_PRIMARY_VALUE, secondaryCounter);
//    }

    public TripleTimestamp(final String siteId, final long primaryCounter,
            final long secondaryCounter) {
        super(siteId, primaryCounter);
        this.secondaryCounter = secondaryCounter;
    }

//    @Override
//    protected boolean hasSecondaryCounter() {
//        return true;
//    }

    /**
     * Returns the size of this object in bytes
     * @return
     */
    public int size() {
        return super.size() + (Long.SIZE / Byte.SIZE);
    }

    @Override
    public long getSecondaryCounter() {
        return secondaryCounter;
    }

    public int hashCode() {
        return super.hashCode() ^ (int) secondaryCounter;
    }

    public String toString() {
        return "(" + getIdentifier() + "," + getCounter() + ","
                + secondaryCounter + ")";
    }

    public Timestamp clone() {
        return new TripleTimestamp(getIdentifier(), getCounter(),
                secondaryCounter);
    }
}
