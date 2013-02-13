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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoCopyable;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A unique id for update on CRDT object with a stable identity and ordering.
 * Ids are logically partitioned in equivalence classes: all ids of updates
 * belonging to the same transaction are in the same equivalence class, and
 * share a client timestamp and system timestamp mapping. This permits to use
 * them as a unique id, but also to refer to the piece of state created by a
 * particular transaction (versioning purpose).
 * <p>
 * Note that the object is immutable wrt. clientTimestamp and the unique
 * component, i.e. the comparison and equals is stable. However, the target
 * system mappings may change, and in that respect instances are thread-hostile.
 * 
 * @author mzawirski
 */
// TODO: provide custom serializer or Kryo-lize the class
final public class TripleTimestamp implements Comparable<TripleTimestamp>, KryoSerializable,
        KryoCopyable<TripleTimestamp> {
    private static final long serialVersionUID = 1L;
    protected long distinguishingCounter;
    protected TimestampMapping mapping;

    /**
     * WARNING Do not use: Empty constructor needed by Kryo
     */
    public TripleTimestamp() {
    }

    TripleTimestamp(final TimestampMapping timestampMapping, final long distinguishingCounter) {
        this.distinguishingCounter = distinguishingCounter;
        this.mapping = timestampMapping;
    }

    /**
     * @return stable client timestamp uniquely identifying the transaction this
     *         timestamp belongs to
     */
    public Timestamp getClientTimestamp() {
        return mapping.getClientTimestamp();
    }

    /**
     * @return timestamp mapping (client<->system) information, including client
     *         timestamp
     */
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

    // FIXME: The following tricks with TimestampMapping sharing are really
    // risky in use. I don't like it (Marek). We should find a safe way to treat
    // them.

    public TripleTimestamp copyWithMappings(final TimestampMapping newMapping) {
        if (!mapping.getClientTimestamp().equals(mapping.getClientTimestamp())) {
            throw new IllegalArgumentException("Invalid mapping to set, it uses different client timestamp");
        }
        return new TripleTimestamp(newMapping, distinguishingCounter);
    }

    public TripleTimestamp copyWithCleanedMappings() {
        return copyWithMappings(new TimestampMapping(getClientTimestamp()));
    }

    /**
     * @see TimestampMapping#anyTimestampIncluded(CausalityClock)
     */
    public boolean timestampsIntersect(CausalityClock clock) {
        return mapping.anyTimestampIncluded(clock);
    }

    /**
     * @see TimestampMapping#addSystemTimestamp(Timestamp)
     */
    public void addSystemTimestamp(Timestamp ts) {
        mapping.addSystemTimestamp(ts);
    }

    /**
     * @see TimestampMapping#addSystemTimestamps(TimestampMapping)
     */
    public void addSystemTimestamps(TimestampMapping mapping) {
        this.mapping.addSystemTimestamps(mapping);
    }

    /**
     * @see TimestampMapping#getSelectedSystemTimestamp()
     */
    public Timestamp getSelectedSystemTimestamp() {
        return mapping.getSelectedSystemTimestamp();
    }

    @Override
    public void read(Kryo kryo, Input in) {
        this.distinguishingCounter = in.readLong();
        this.mapping = new TimestampMapping();
        this.mapping.read(kryo, in);
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeLong(this.distinguishingCounter);
        mapping.write(kryo, out);
    }

    @Override
    public TripleTimestamp copy(Kryo kryo) {
        return new TripleTimestamp(this.mapping, this.distinguishingCounter);
    }
}
