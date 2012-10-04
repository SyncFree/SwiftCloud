package swift.clocks;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.crdt.interfaces.Copyable;

/**
 * Timestamp information for a transaction, with a stable client timestamp and a
 * grow-only set of system timestamps, added with each successful run of the
 * hand-off protocol.
 * <p>
 * Thread-hostile, synchronize externally if really needed. Only
 * {@link #getClientTimestamp()} is immutable.
 * 
 * @author mzawirski
 */
public class TimestampMapping implements Copyable {
    /** Stable client-assigned timestamp */
    protected Timestamp clientTimestamp;
    /** Sorted client- and all system-assigned timestamps */
    protected List<Timestamp> timestamps;

    /**
     * USED ONLY BY Kyro!
     */
    public TimestampMapping() {
    }

    /**
     * Create an instance with only client timestamp defined.
     * 
     * @param clientTimestamp
     *            stable client timestamp to use
     */
    public TimestampMapping(Timestamp clientTimestamp) {
        this.clientTimestamp = clientTimestamp;
        this.timestamps = new LinkedList<Timestamp>();
        timestamps.add(clientTimestamp);
    }

    /**
     * @return stable client timestamp for the transaction
     */
    public Timestamp getClientTimestamp() {
        return clientTimestamp;
    }

    /**
     * @return unmodifiable list of all timestamps assigned to the transaction
     */
    public List<Timestamp> getTimestamps() {
        return Collections.unmodifiableList(timestamps);
    }

    /**
     * @return selected system timestamp for the transaction; deterministic and
     *         stable given the same final set of timestamp mappings
     * @throws IllegalStateException
     *             when there is no system timestamp defined for the transaction
     */
    public Timestamp getSelectedSystemTimestamp() {
        for (final Timestamp ts : timestamps) {
            // Pick the first non-client timestamp.
            if (!ts.equals(clientTimestamp)) {
                return ts;
            }
        }
        throw new IllegalStateException("No system timestamp defined for this instance");
    }

    /**
     * Checks whether the provided clock includes any timestamp used by this
     * update id.
     * <p>
     * When it returns true, all subsequent calls will also yield true
     * (timestamp mappings can only grow).
     * 
     * @param clock
     *            clock to check against
     * @return true if any timestamp (client or system) used to represent the
     *         transaction of this update intersects with the provided clock
     */
    public boolean timestampsIntersect(final CausalityClock clock) {
        for (final Timestamp ts : timestamps) {
            if (clock.includes(ts)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds a new system timestamp mapping for the transaction. Idempotent.
     * 
     * @param ts
     *            system timestamp to add
     */
    public void addSystemTimestamp(final Timestamp ts) {
        final int idx = Collections.binarySearch(timestamps, ts);
        if (idx < 0) {
            timestamps.add((idx + 1) * -1, ts);
        }
    }

    /**
     * Adds all provided mappings..
     * 
     * @param mapping
     *            transaction mappings to merge, must use the same client
     *            timestamp
     */
    public void addSystemTimestamps(TimestampMapping otherMapping) {
        if (!getClientTimestamp().equals(otherMapping.getClientTimestamp())) {
            throw new IllegalArgumentException("Invalid mappings to merge, they use different client timestamp");
        }
        for (final Timestamp ts : otherMapping.getTimestamps()) {
            if (ts.equals(otherMapping.getClientTimestamp())) {
                continue;
            }
            addSystemTimestamp(ts);
        }
    }

    @Override
    public TimestampMapping copy() {
        final TimestampMapping copy = new TimestampMapping(clientTimestamp);
        copy.timestamps.clear();
        copy.timestamps.addAll(timestamps);
        return copy;
    }
}
