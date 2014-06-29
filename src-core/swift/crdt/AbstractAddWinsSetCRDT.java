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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;
import swift.utils.PrettyPrint;

/**
 * Abstract add-wins set CRDT, independent of set type (ordered vs unordered).
 * 
 * @author vb, annettebieniusa, mzawirsk
 * 
 * @param <V>
 *            elements type
 * @param <T>
 *            type of concrete implementation
 */
public abstract class AbstractAddWinsSetCRDT<V, T extends AbstractAddWinsSetCRDT<V, T>> extends BaseCRDT<T> implements
        KryoSerializable {

    // Kryo
    protected AbstractAddWinsSetCRDT() {
    }

    protected AbstractAddWinsSetCRDT(final CRDTIdentifier id) {
        super(id);
    }

    protected AbstractAddWinsSetCRDT(final CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock) {
        super(id, txn, clock);
    }

    protected abstract void createElementsInstances();

    protected abstract Map<V, Set<TripleTimestamp>> getElementsInstances();

    /**
     * @return non-modifiable reference of a set of elements
     */
    @Override
    public Set<V> getValue() {
        return Collections.unmodifiableSet(getElementsInstances().keySet());
    }

    public int size() {
        return getElementsInstances().size();
    }

    public boolean lookup(V element) {
        return getElementsInstances().containsKey(element);
    }

    public void add(final V element) {
        final TripleTimestamp ts = nextTimestamp();
        final Set<TripleTimestamp> existingInstances = AddWinsUtils.add(getElementsInstances(), element, ts);
        registerLocalOperation(generateUpdateDownstream(element, ts, existingInstances));
    }

    protected AddWinsSetUpdate<V, T> generateUpdateDownstream(final V element, final TripleTimestamp ts,
            final Set<TripleTimestamp> existingInstances) {
        return new AddWinsSetUpdate<V, T>(element, ts, existingInstances);
    }

    public void remove(V element) {
        Set<TripleTimestamp> removedInstances = AddWinsUtils.remove(getElementsInstances(), element);
        if (removedInstances != null) {
            registerLocalOperation(generateUpdateDownstream(element, null, removedInstances));
        }
    }

    protected void applyUpdate(V element, TripleTimestamp instance, Set<TripleTimestamp> overwrittenInstances) {
        AddWinsUtils.applyUpdate(getElementsInstances(), element, instance, overwrittenInstances);
    }

    @Override
    public String toString() {
        return PrettyPrint.printMap("{", "}", ";", "->", getElementsInstances());
    }

    @Override
    public void write(Kryo kryo, Output output) {
        baseWrite(kryo, output);
        output.writeVarInt(getElementsInstances().size(), true);
        for (Entry<V, Set<TripleTimestamp>> entry : getElementsInstances().entrySet()) {
            writeElement(kryo, output, entry.getKey());
            final Set<TripleTimestamp> timestamps = entry.getValue();
            output.writeVarInt(timestamps.size(), true);
            for (final TripleTimestamp ts : timestamps) {
                ts.write(kryo, output);
            }
        }
    }

    protected void writeElement(Kryo kryo, Output output, V element) {
        kryo.writeClassAndObject(output, element);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        baseRead(kryo, input);
        final int elementsNumber = input.readVarInt(true);
        createElementsInstances();
        final Map<V, Set<TripleTimestamp>> elemsInstances = getElementsInstances();
        for (int i = 0; i < elementsNumber; i++) {
            final V element = readElement(kryo, input);
            final int timestampsNumber = input.readVarInt(true);
            final HashSet<TripleTimestamp> timestamps = new HashSet<>();
            for (int j = 0; j < timestampsNumber; j++) {
                final TripleTimestamp ts = new TripleTimestamp();
                ts.read(kryo, input);
                timestamps.add(ts);
            }
            elemsInstances.put(element, timestamps);
        }
    }

    @SuppressWarnings("unchecked")
    protected V readElement(Kryo kryo, Input input) {
        return (V) kryo.readClassAndObject(input);
    }
}
