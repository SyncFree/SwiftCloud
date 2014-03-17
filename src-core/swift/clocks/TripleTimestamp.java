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
 * An immutable unique id for update on CRDT object with a stable identity and
 * ordering. Ids are logically partitioned in equivalence classes: all ids of
 * updates belonging to the same transaction are in the same equivalence class,
 * and share a client timestamp and system timestamp mapping. This permits to
 * use them as a unique id, but also to refer to the piece of state created by a
 * particular transaction (versioning purpose).
 * 
 * @author mzawirsk
 */
final public class TripleTimestamp implements Comparable<TripleTimestamp>, KryoSerializable,
        KryoCopyable<TripleTimestamp> {
    private static final long serialVersionUID = 1L;
    /** Stable client-assigned timestamp (transaction id) */
    protected Timestamp clientTimestamp;
    /** Stable component within a transaction */
    protected long distinguishingCounter;

    /**
     * WARNING Do not use: Empty constructor needed by Kryo
     */
    public TripleTimestamp() {
    }

    TripleTimestamp(final Timestamp clientTimestamp, final long distinguishingCounter) {
        this.clientTimestamp = clientTimestamp;
        this.distinguishingCounter = distinguishingCounter;
    }

    /**
     * @return stable client timestamp uniquely identifying the transaction this
     *         timestamp belongs to
     */
    public Timestamp getClientTimestamp() {
        return clientTimestamp;
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
                + distinguishingCounter + ")";
    }

    @Override
    public void read(Kryo kryo, Input in) {
        this.distinguishingCounter = in.readLong();
        this.clientTimestamp = new Timestamp();
        this.clientTimestamp.read(kryo, in);
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeLong(this.distinguishingCounter);
        clientTimestamp.write(kryo, out);
    }

    @Override
    public TripleTimestamp copy(Kryo kryo) {
        return new TripleTimestamp(this.clientTimestamp, this.distinguishingCounter);
    }
}
