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
package swift.crdt;

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTUpdate;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LWWRegisterUpdate<V, T extends AbstractLWWRegisterCRDT<V, T>> implements CRDTUpdate<T>,
        Comparable<LWWRegisterUpdate<V, T>>, KryoSerializable {
    protected V val;
    protected long registerTimestamp;
    protected TripleTimestamp tiebreakingTimestamp;

    // required for kryo
    public LWWRegisterUpdate() {
    }

    public LWWRegisterUpdate(long registerTimestamp, TripleTimestamp tiebreakingTimestamp, V val) {
        this.registerTimestamp = registerTimestamp;
        this.tiebreakingTimestamp = tiebreakingTimestamp;
        this.val = val;
    }

    @Override
    public void applyTo(T register) {
        register.applySet(this);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return val;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeBoolean(val != null);
        if (val != null) {
            writeValue(kryo, output);
        }
        output.writeVarLong(registerTimestamp, true);
        tiebreakingTimestamp.write(kryo, output);
    }

    protected void writeValue(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, val);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        if (input.readBoolean()) {
            readValue(kryo, input);
        }
        registerTimestamp = input.readVarLong(true);
        tiebreakingTimestamp = new TripleTimestamp();
        tiebreakingTimestamp.read(kryo, input);
    }

    protected void readValue(Kryo kryo, Input input) {
        val = (V) kryo.readClassAndObject(input);
    }

    @Override
    public int compareTo(LWWRegisterUpdate<V, T> o) {
        if (this.registerTimestamp != o.registerTimestamp) {
            return Long.signum(this.registerTimestamp - o.registerTimestamp);
        }
        if (this.tiebreakingTimestamp == null && o.tiebreakingTimestamp != null) {
            return -1;
        }
        if (this.tiebreakingTimestamp != null && o.tiebreakingTimestamp == null) {
            return 1;
        }
        if (this.tiebreakingTimestamp == null && o.tiebreakingTimestamp == null) {
            return 0;
        }
        return this.tiebreakingTimestamp.compareTo(o.tiebreakingTimestamp);
    }
}
