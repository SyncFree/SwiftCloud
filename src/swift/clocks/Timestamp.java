package swift.clocks;

import java.io.Serializable;

/**
 * Common base class for timestamps using 1-2 dimensional site counters, with
 * default implementation for 1 dimension.
 * <p>
 * Instances of this class may not be immutable.
 * <p>
 * TODO: provide some sealing mechanism for eventual immutability.
 * <p>
 * TODO: describe the role of secondary dimension, events coalescing.
 * 
 * @see TripleTimestamp
 */
public class Timestamp implements Serializable, Comparable<Timestamp> {
    private static final long serialVersionUID = 1L;
    private final String siteid;
    private long counter;

    public Timestamp(String siteid, long counter) {
        this.siteid = siteid;
        this.counter = counter;
    }

    @Override
    public int hashCode() {
        return siteid.hashCode() ^ (int) counter;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Timestamp)) {
            return false;
        }
        return compareTo((Timestamp) obj) == 0;
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
        if (getSecondaryCounter() != ot.getSecondaryCounter()) {
            return Long.signum(getSecondaryCounter() - ot.getSecondaryCounter());
        }
        return siteid.compareTo(ot.siteid);
    }

    /**
     * Returns the size of this object in bytes
     * @return
     */
    public int size() {
        return (Long.SIZE / Byte.SIZE) + siteid.length();
    }

    /**
     * @return the site identifier
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

//    /**
//     * @param value
//     *            new primary counter value
//     */
//    @Deprecated
//    public void setCounter(final long value) {
//        this.counter = value;
//    }

//    /**
//     * @return true if this timestamp contains secondary counter
//     */
//    protected boolean hasSecondaryCounter() {
//        return false;
//    }

    /**
     * @return value of the secondary counter, meaningful if
     *         {@link #hasSecondaryCounter()}
     */
    public long getSecondaryCounter() {
        return -1;
    }
}
