package swift.clocks;

// TODO: provide custom serializer or Kryo-lize the class
public class TripleTimestamp implements Comparable<TripleTimestamp> {
    private static final long serialVersionUID = 1L;
    protected Timestamp clientTimestamp;
    protected long distinguishingCounter;
    protected TimestampMapping mapping;

    /**
     * DO NOT USE: Kryo-hack constructor.
     */
    public TripleTimestamp() {
    }

    TripleTimestamp(final Timestamp clientTimestamp, final long distinguishingCounter) {
        this.clientTimestamp = clientTimestamp;
        this.distinguishingCounter = distinguishingCounter;
        this.mapping = new TimestampMapping(clientTimestamp);
    }

    public Timestamp getClientTimestamp() {
        return clientTimestamp;
    }

    public void setMapping(TimestampMapping mapping) {
        if (!clientTimestamp.equals(mapping.getClientTimestamp())) {
            throw new IllegalArgumentException("Attempt to assign incompatible timestamp mapping");
        }
        this.mapping = mapping;
    }

    public TimestampMapping getMapping() {
        return mapping;
    }

    @Override
    public int compareTo(TripleTimestamp o) {
        final int tsResult = clientTimestamp.compareTo(o.clientTimestamp);
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
        return clientTimestamp.hashCode() ^ (int) distinguishingCounter;
    }

    public String toString() {
        return "(" + clientTimestamp.getIdentifier() + "," + clientTimestamp.getCounter() + "," + distinguishingCounter
                + "," + mapping.toString() + ")";
    }

    public TripleTimestamp copy() {
        final TripleTimestamp copy = new TripleTimestamp(clientTimestamp, distinguishingCounter);
        copy.mapping = mapping.copy();
        return copy;
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
}
