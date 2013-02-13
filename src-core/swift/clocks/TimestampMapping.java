/*****************************************************************************
 * Copyright 2011-2012 INRIA
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

import java.util.Arrays;

import swift.crdt.interfaces.Copyable;

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
    /** Stable client-assigned timestamp */
    protected Timestamp clientTimestamp;
    /** Sorted client- and all system-assigned timestamps */
    protected Timestamp[] timestamps;

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
        this.clientTimestamp = clientTimestamp;
        this.timestamps = new Timestamp[] { clientTimestamp };
    }

    /**
     * Create new instance from the supplied data
     * 
     * @param clientTimestamp
     *            stable client timestamp to use
     * @param timestamps
     *            stable system timestamps to use
     */
    public TimestampMapping(Timestamp clientTimestamp, Timestamp[] timestamps) {
        this.clientTimestamp = clientTimestamp;
        this.timestamps = Arrays.copyOf(timestamps, timestamps.length);
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
    public Timestamp[] getTimestamps() {
        return timestamps; // smd Arrays.copyOf(...) to ensure imutability????
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
        for (final Timestamp ts : timestamps) {
            if (ts.equals(clientTimestamp)) {
                continue;
            }
            if (!clock.includes(ts)) {
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
        final int idx = Arrays.binarySearch(timestamps, ts);
        if (idx < 0) {
            int j = (idx + 1) * -1;
            Timestamp[] old = timestamps;
            timestamps = Arrays.copyOf(old, old.length + 1);
            timestamps[j] = ts;
            if (j < old.length)
                System.arraycopy(old, j, timestamps, j + 1, old.length - j);
            ;
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

    /**
     * @return true when there is at least 1 system timestamp defined
     */
    public boolean hasSystemTimestamp() {
        return timestamps.length > 1;
    }

    @Override
    public TimestampMapping copy() {
        return new TimestampMapping(this.clientTimestamp, this.timestamps);
    }

    @Override
    public String toString() {
        return getTimestamps().toString();
    }

    @Override
    public void read(Kryo kryo, Input in) {
        this.clientTimestamp = new Timestamp();
        this.clientTimestamp.read(kryo, in);
        int n = in.readByte();
        this.timestamps = new Timestamp[n];
        for (int i = 0; i < n; i++) {
            this.timestamps[i] = new Timestamp();
            this.timestamps[i].read(kryo, in);
        }
    }

    @Override
    public void write(Kryo kryo, Output out) {
        this.clientTimestamp.write(kryo, out);
        out.writeByte(this.timestamps.length);
        for (Timestamp i : timestamps)
            i.write(kryo, out);
    }

    @Override
    public TimestampMapping copy(Kryo kryo) {
        return new TimestampMapping(this.clientTimestamp, this.timestamps);
    }
}
