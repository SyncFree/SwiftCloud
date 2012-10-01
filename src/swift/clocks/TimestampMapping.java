package swift.clocks;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

// FIXME: make it thread-safe?
public class TimestampMapping {
    /** Stable client-assigned timestamp */
    protected Timestamp clientTimestamp;
    /** Sorted client- and all system-assigned timestamps */
    protected List<Timestamp> timestamps;

    public TimestampMapping(Timestamp clientTimestamp) {
        this.clientTimestamp = clientTimestamp;
        this.timestamps = new LinkedList<Timestamp>();
        timestamps.add(clientTimestamp);
    }

    public Timestamp getClientTimestamp() {
        return clientTimestamp;
    }

    public List<Timestamp> getTimestamps() {
        return Collections.unmodifiableList(timestamps);
    }

    public Timestamp getSelectedSystemTimestamp() {
        for (final Timestamp ts : timestamps) {
            // Pick the first non-client timestamp.
            if (!ts.equals(clientTimestamp)) {
                return ts;
            }
        }
        throw new IllegalStateException("No system timestamp defined for this instance");
    }

    public boolean timestampsIntersect(final CausalityClock clock) {
        for (final Timestamp ts : timestamps) {
            if (clock.includes(ts)) {
                return true;
            }
        }
        return false;
    }

    public void addSystemTimestamp(final Timestamp ts) {
        final int idx = Collections.binarySearch(timestamps, ts);
        if (idx < 0) {
            timestamps.add((idx + 1) * -1, ts);
        }
    }

    public void addSystemTimestamps(TimestampMapping otherMapping) {
        for (final Timestamp ts : otherMapping.getTimestamps()) {
            if (ts.equals(otherMapping.getClientTimestamp())) {
                continue;
            }
            addSystemTimestamp(ts);
        }
    }

    public TimestampMapping copy() {
        final TimestampMapping copy = new TimestampMapping(clientTimestamp);
        copy.timestamps.addAll(timestamps);
        return copy;
    }
}
