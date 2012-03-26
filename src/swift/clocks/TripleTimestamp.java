package swift.clocks;


/**
 * Timestamp implementation with two-dimensional counter (which accounts for
 * triple together with siteId).
 */
// TODO: provide custom serializer or Kryo-lize the class
public class TripleTimestamp extends Timestamp {
    private static final long serialVersionUID = 1L;
    protected long secondaryCounter;

    public TripleTimestamp() {
    }
    /**
     * Creates triple counter with default maximum value for primary counter.
     * 
     * @param siteId
     * @param primaryCounter
     * @param secondaryCounter
     */
    TripleTimestamp(final String siteId, final long primaryCounter, final long secondaryCounter) {
        super(siteId, primaryCounter);
        this.secondaryCounter = secondaryCounter;
    }

    /**
     * Returns the size of this object in bytes
     * 
     * @return
     */
    public int size() {
        return super.size() + (Long.SIZE / Byte.SIZE);
    }

    @Override
    public long getSecondaryCounter() {
        return secondaryCounter;
    }

    /**
     * Returns true if this timestamp is equal to the given Timestamp. If the
     * given object is a TripleTimestamp, returns true if they share the same
     * base timestamp.
     */
    public boolean includes(Object obj) {
        if (!(obj instanceof TripleTimestamp)) {
            return false;
        }
        return compareTo((Timestamp) obj) == 0;
    }

    public int hashCode() {
        return super.hashCode() ^ (int) secondaryCounter;
    }

    public String toString() {
        return "(" + getIdentifier() + "," + getCounter() + "," + secondaryCounter + ")";
    }

    public Timestamp clone() {
        return new TripleTimestamp(getIdentifier(), getCounter(), secondaryCounter);
    }

    /**
     * Creates a copy of this timestamp using a different base timestamp
     * (identifiers and counter).
     * 
     * @param baseTimestamp
     *            base timestamp - source of identifier and counter
     * @return a copy of this timestamp with the provided base
     */
    public TripleTimestamp withBaseTimestamp(final Timestamp baseTimestamp) {
        return new TripleTimestamp(baseTimestamp.getIdentifier(), baseTimestamp.getCounter(), getSecondaryCounter());
    }
}
