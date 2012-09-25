package swift.clocks;

import java.io.Serializable;

/**
 * Common base class for timestamps using 1-2 dimensional site counters, with
 * default implementation for 1 dimension.
 * <p>
 * Instances of this class are immutable.
 * 
 * @see TripleTimestamp
 */
public class Timestamp implements Serializable, Comparable<Timestamp> {
    /**
     * Minimum counter value (exclusive!?), never used by any timestamp.
     */
    public static final long MIN_VALUE = 0L;

    private static final long serialVersionUID = 1L;
    private String siteid;
    private long counter;

    /**
     * Do not use: Empty constructor needed by Kryo
     */
    public Timestamp() {
    }

    Timestamp(String siteid, long counter) {
        this.siteid = siteid;
        this.counter = counter;
    }

    @Override
    public int hashCode() {
        return siteid.hashCode() ^ (int) counter;
    }

    /**
     * Returns true if objects represent the same timestamp
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof Timestamp)) {
            return false;
        }
        return compareTo((Timestamp) obj) == 0;
    }

    /**
     * Returns true if this timestamp includes the given Timestamp. If the given
     * object is a Timestamp, returns true if they are the same timestamp. If
     * the given object is a TripleTimestamp, returns true if the given
     * Timestamp has the same objects are
     */
    public boolean includes(Object obj) {
        if (!(obj instanceof Timestamp)) {
            return false;
        }
        Timestamp ot = (Timestamp) obj;
        return getCounter() == ot.getCounter() && siteid.equals(ot.getIdentifier());
    }

    public String toString() {
        return "(" + siteid + "," + counter + ")";
    }

    public Timestamp clone() {
        return new Timestamp(this.siteid, this.counter);
    }

    /**
     * Compares two timestamps, possibly of different dimensions. This
     * implementations compares only common dimensions and then looks at siteId.
     */
    public int compareTo(Timestamp ot) {
        if (getCounter() != ot.getCounter()) {
            return Long.signum(getCounter() - ot.getCounter());
        }
        return siteid.compareTo(ot.siteid);
    }

    /**
     * 
     * @return size of the timestamp (in bytes)
     */
    public int size() {
        return (Long.SIZE / Byte.SIZE) + siteid.length();
    }

    /**
     * @return site identifier for the timestamp
     */
    public String getIdentifier() {
        return siteid;
    }

    /**
     * @return value of the primary counter
     */
    public long getCounter() {
        return counter;
    }
}
