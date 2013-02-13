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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoCopyable;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Common base class for timestamps using 1-2 dimensional site counters, with
 * default implementation for 1 dimension.
 * <p>
 * Instances of this class are immutable.
 * 
 * @see TripleTimestamp
 */
final public class Timestamp implements Serializable, Comparable<Timestamp>, KryoSerializable, KryoCopyable<Timestamp> {
    /**
     * Minimum counter value (exclusive!?), never used by any timestamp.
     */
    public static final long MIN_VALUE = 0L;

    private static final long serialVersionUID = 1L;
    private String siteid;
    private long counter;

    /**
     * WARNING: Do not use: Empty constructor needed by Kryo
     */
    public Timestamp() {
    }

    public Timestamp(String siteid, long counter) {
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

    /************ FOR KRYO ***************/
    @Override
    public void read(Kryo kryo, Input in) {
        this.siteid = s2s(in.readString());
        this.counter = in.readLong();
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeString(this.siteid);
        out.writeLong(this.counter);
    }

    @Override
    public Timestamp copy(Kryo kryo) {
        return new Timestamp(this.siteid, this.counter);
    }

    String s2s(String s) {
        String ref = s2s.get(s);
        if (ref == null)
            s2s.put(s, ref = s);
        return ref;
    }

    private static Map<String, String> s2s = new HashMap<String, String>();
}
