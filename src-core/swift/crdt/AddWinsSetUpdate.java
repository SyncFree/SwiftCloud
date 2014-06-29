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

import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTUpdate;

public class AddWinsSetUpdate<V, T extends AbstractAddWinsSetCRDT<V, T>> implements CRDTUpdate<T>, KryoSerializable {
    protected V val;
    protected TripleTimestamp newInstance;
    // WISHME: represent it more efficiently using vectors if possible.
    // That would require some substantial API chances, since it's not as easy
    // as using dependenceClock (for example, it can be overapproximated), there
    // are holes in here as well.
    protected Set<TripleTimestamp> removedInstances;

    // required for kryo
    public AddWinsSetUpdate() {
    }

    public AddWinsSetUpdate(V val, TripleTimestamp newInstance, Set<TripleTimestamp> removedInstances) {
        this.val = val;
        this.newInstance = newInstance;
        this.removedInstances = removedInstances;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.applyUpdate(val, newInstance, removedInstances);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return val;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeElement(kryo, output);
        if (newInstance == null) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            newInstance.write(kryo, output);
        }
        if (removedInstances == null) {
            output.writeVarInt(0, true);
        } else {
            output.writeVarInt(removedInstances.size(), true);
            for (final TripleTimestamp overwrittenInstance : removedInstances) {
                overwrittenInstance.write(kryo, output);
            }
        }
    }

    protected void writeElement(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, val);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readElement(kryo, input);
        if (input.readBoolean()) {
            newInstance = new TripleTimestamp();
            newInstance.read(kryo, input);
        }
        final int removedInstancesSize = input.readVarInt(true);
        removedInstances = new HashSet<>(removedInstancesSize);
        for (int i = 0; i < removedInstancesSize; i++) {
            TripleTimestamp overwrittenInstance = new TripleTimestamp();
            overwrittenInstance.read(kryo, input);
            removedInstances.add(overwrittenInstance);
        }
    }

    @SuppressWarnings("unchecked")
    protected void readElement(Kryo kryo, Input input) {
        val = (V) kryo.readClassAndObject(input);
    }
}
