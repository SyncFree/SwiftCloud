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

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * A "bloated" implementation of integer counter with addition and subtraction
 * operations, which does not make use of reliable causal delivery.
 * 
 * @author mzawirsk
 */
public class BloatedIntegerCRDT extends BaseCRDT<BloatedIntegerCRDT> {
    protected Map<String, Integer> increments;
    protected Map<String, Integer> decrements;
    protected int observableValue;

    // Kryo
    public BloatedIntegerCRDT() {
    }

    public BloatedIntegerCRDT(CRDTIdentifier uid) {
        super(uid);
        increments = new HashMap<>();
        decrements = new HashMap<>();
        observableValue = 0;
    }

    private BloatedIntegerCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Map<String, Integer> increments,
            Map<String, Integer> decrements, int observableValue) {
        super(id, txn, clock);
        this.increments = increments;
        this.decrements = decrements;
        this.observableValue = observableValue;
    }

    public Integer getValue() {
        return observableValue;
    }

    public void add(int n) {
        doDelta(n);
    }

    public void sub(int n) {
        doDelta(-n);
    }

    private void doDelta(int delta) {
        observableValue += delta;
        final boolean positive = delta >= 0;
        final Map<String, Integer> values = getValues(positive);
        final TripleTimestamp ts = nextTimestamp();
        final String clientId = ts.getClientTimestamp().getIdentifier();
        Integer clientValue = values.get(clientId);
        if (clientValue == null) {
            clientValue = 0;
        }
        clientValue += Math.abs(delta);
        values.put(clientId, clientValue);
        // WISHME: this would be a cool case for coalescing many updates.
        registerLocalOperation(new BloatedIntegerUpdate(clientId, clientValue, positive));
    }

    private Map<String, Integer> getValues(boolean positive) {
        if (positive) {
            return increments;
        }
        return decrements;
    }

    /**
     * Applies donwstream add. No assumptions on reliable delivery.
     * 
     * @param delta
     */
    protected void applyClientUpdate(String clientId, int value, boolean positive) {
        final Map<String, Integer> values = getValues(positive);
        Integer oldClientValue = values.get(clientId);
        if (oldClientValue == null) {
            oldClientValue = 0;
        }
        values.put(clientId, value);
        if (positive) {
            observableValue += value - oldClientValue;
        } else {
            observableValue -= value - oldClientValue;
        }
    }

    @Override
    public BloatedIntegerCRDT copy() {
        return new BloatedIntegerCRDT(id, getTxnHandle(), getClock(), new HashMap<>(increments), new HashMap<>(
                decrements), observableValue);
    }
}
