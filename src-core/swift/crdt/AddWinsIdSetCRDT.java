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
package swift.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Add-wins set CRDT, optimized storing {@link CRDTIdentifier}s only.
 * 
 * @author mzawirski
 */
public class AddWinsIdSetCRDT extends AbstractAddWinsSetCRDT<CRDTIdentifier, AddWinsIdSetCRDT> implements
        KryoSerializable {
    protected Map<CRDTIdentifier, Set<TripleTimestamp>> elemsInstances;

    // Kryo
    public AddWinsIdSetCRDT() {
    }

    public AddWinsIdSetCRDT(CRDTIdentifier id) {
        super(id);
        createElementsInstances();
    }

    private AddWinsIdSetCRDT(CRDTIdentifier id, final TxnHandle txn, final CausalityClock clock) {
        super(id, txn, clock);
        createElementsInstances();
    }

    @Override
    protected void createElementsInstances() {
        elemsInstances = new HashMap<>();
    }

    @Override
    protected Map<CRDTIdentifier, Set<TripleTimestamp>> getElementsInstances() {
        return elemsInstances;
    }

    @Override
    public AddWinsIdSetCRDT copy() {
        AddWinsIdSetCRDT copy = new AddWinsIdSetCRDT(id, txn, clock);
        AddWinsUtils.deepCopy(elemsInstances, copy.elemsInstances);
        return copy;
    }
    
    @Override
    protected AddWinsSetUpdate<CRDTIdentifier, AddWinsIdSetCRDT> generateUpdateDownstream(CRDTIdentifier element,
            TripleTimestamp ts, Set<TripleTimestamp> existingInstances) {
        return new AddWinsIdSetUpdate(element, ts, existingInstances);
    }

    @Override
    protected void writeElement(Kryo kryo, Output output, CRDTIdentifier element) {
        element.write(kryo, output);
    }

    @Override
    protected CRDTIdentifier readElement(Kryo kryo, Input input) {
        final CRDTIdentifier element = new CRDTIdentifier();
        element.read(kryo, input);
        return element;
    }
}
