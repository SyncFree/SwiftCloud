/*****************************************************************************
 * Copyright 2011-2014 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.clocks;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import swift.crdt.core.Copyable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoCopyable;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
final public class TimestampMapping implements Copyable, KryoSerializable, KryoCopyable<TimestampMapping> {
    /** Client timestamp followed by system-assigned timestamps */
    protected LinkedList<Timestamp> timestamps;

    /**
     * USED by Kyro and copy
     */
    TimestampMapping() {
    }

    /**
     * Create an instance with only client timestamp defined.
     * 
     * @param clientTimestamp
     *            stable client timestamp to use
     */
    public TimestampMapping(Timestamp clientTimestamp) {
        this.timestamps = new LinkedList<Timestamp>();
        this.timestamps.add(clientTimestamp);
    }

    private TimestampMapping(List<Timestamp> timestamps) {
        this.timestamps = new LinkedList<Timestamp>(timestamps);
    }

    /**
     * @return stable client timestamp for the transaction
     */
    public Timestamp getClientTimestamp() {
        return timestamps.get(0);
    }

    /**
     * @return unmodifiable list of all timestamps assigned to the transaction
     */
    public List<Timestamp> getTimestamps() {
        return Collections.unmodifiableList(timestamps);
    }

    /**
     * @return unmodifiable list of all system timestamps assigned to the
     *         transaction
     */
    public List<Timestamp> getSystemTimestamps() {
        return Collections.unmodifiableList(timestamps.subList(1, timestamps.size()));
    }

    /**
     * Checks whether the provided clock includes any timestamp used by this
     * update id.
     * <p>
     * Property: when it returns true, all subsequent calls will also yield true
     * (timestamp mappings can only grow).
     * 
     * @param clock
     *            clock to check against
     * @return true if any timestamp (client or system) used to represent the
     *         transaction of this update intersects with the provided clock
     */
    public boolean anyTimestampIncluded(final CausalityClock clock) {
        for (final Timestamp ts : timestamps) {
            if (clock.includes(ts)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the provided clock includes all system timestamps used by
     * this update id.
     * 
     * @param clock
     *            clock to check against
     * @return true if all system timestamps used to represent the transaction
     *         of this update intersects with the provided clock
     */
    public boolean allSystemTimestampsIncluded(final CausalityClock clock) {
        final Iterator<Timestamp> iter = timestamps.iterator();
        // Skip client timestamp.
        iter.next();
        while (iter.hasNext()) {
            if (!clock.includes(iter.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds a new system timestamp mapping for the transaction. Idempotent.
     * 
     * @param ts
     *            system timestamp to add
     */
    public void addSystemTimestamp(final Timestamp ts) {
        if (timestamps.contains(ts)) {
            return;
        }
        timestamps.add(ts);
    }

    /**
     * Merges systems timestamps from the provided mapping. Idempotent.
     * 
     * @param mapping
     *            transaction mappings to merge with, must use the same client
     *            timestamp; remains unchanged
     */
    public void mergeIn(TimestampMapping otherMapping) {
        final Iterator<Timestamp> iter = otherMapping.timestamps.iterator();
        if (!getClientTimestamp().equals(iter.next())) {
            throw new IllegalArgumentException("Invalid mappings to merge, they use different client timestamp");
        }
        while (iter.hasNext()) {
            addSystemTimestamp(iter.next());
        }
    }

    /**
     * @return true when there is at least 1 system timestamp defined
     */
    public boolean hasSystemTimestamp() {
        return timestamps.size() > 1;
    }

    @Override
    public TimestampMapping copy() {
        return new TimestampMapping(this.timestamps);
    }

    @Override
    public String toString() {
        return timestamps.toString();
    }

    @Override
    public void read(Kryo kryo, Input in) {
        int n = in.readByte();
        this.timestamps = new LinkedList<Timestamp>();
        for (int i = 0; i < n; i++) {
            final Timestamp ts = new Timestamp();
            ts.read(kryo, in);
            timestamps.add(ts);
        }
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeByte(this.timestamps.size());
        for (Timestamp i : timestamps)
            i.write(kryo, out);
    }

    @Override
    public TimestampMapping copy(Kryo kryo) {
        return new TimestampMapping(this.timestamps);
    }

    @Override
    public int hashCode() {
        return timestamps.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TimestampMapping)) {
            return false;
        }
        return timestamps.equals(((TimestampMapping) obj).timestamps);
    }
}
