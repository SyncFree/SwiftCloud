package swift.clocks;

// TODO: provide custom serializer or Kryo-lize the class
public class TripleTimestamp implements Comparable<TripleTimestamp> {
    private static final long serialVersionUID = 1L;
    protected long distinguishingCounter;
    protected TimestampMapping mapping;

    /**
     * DO NOT USE: Kryo-hack constructor.
     */
    public TripleTimestamp() {
    }

    TripleTimestamp(final TimestampMapping timestampMapping, final long distinguishingCounter) {
        this.distinguishingCounter = distinguishingCounter;
        this.mapping = timestampMapping;
    }

    public Timestamp getClientTimestamp() {
        return mapping.getClientTimestamp();
    }

    public void setMapping(TimestampMapping mapping) {
        if (!getClientTimestamp().equals(mapping.getClientTimestamp())) {
            throw new IllegalArgumentException("Attempt to assign incompatible timestamp mapping");
        }
        this.mapping = mapping;
    }

    public TimestampMapping getMapping() {
        return mapping;
    }

    @Override
    public int compareTo(TripleTimestamp o) {
        final int tsResult = getClientTimestamp().compareTo(o.getClientTimestamp());
        if (tsResult != 0) {
            return tsResult;
        }
        return Long.signum(distinguishingCounter - o.distinguishingCounter);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TripleTimestamp)) {
            return false;
        }
        return compareTo((TripleTimestamp) obj) == 0;
    }

    public int hashCode() {
        return getClientTimestamp().hashCode() ^ (int) distinguishingCounter;
    }

    public String toString() {
        return "(" + getClientTimestamp().getIdentifier() + "," + getClientTimestamp().getCounter() + ","
                + distinguishingCounter + "," + mapping.toString() + ")";
    }

    public TripleTimestamp copy() {
        return new TripleTimestamp(mapping.copy(), distinguishingCounter);
    }

    public boolean timestampsIntersect(CausalityClock clock) {
        return mapping.timestampsIntersect(clock);
    }

    public void addSystemTimestamp(Timestamp ts) {
        mapping.addSystemTimestamp(ts);
    }

    public void addSystemTimestamps(TimestampMapping mapping) {
        mapping.addSystemTimestamps(mapping);
    }

    public Timestamp getSelectedSystemTimestamp() {
        return mapping.getSelectedSystemTimestamp();
    }
}
