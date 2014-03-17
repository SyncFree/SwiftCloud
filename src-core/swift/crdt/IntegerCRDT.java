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

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * Integer counter with addition and subtraction operations.
 * 
 * @author mzawirsk
 */
public class IntegerCRDT extends BaseCRDT<IntegerCRDT> {
    protected int val;

    // Kryo
    public IntegerCRDT() {
    }

    public IntegerCRDT(CRDTIdentifier uid) {
        super(uid);
        // default value is just right
    }

    private IntegerCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int val) {
        super(id, txn, clock);
        this.val = val;
    }

    public Integer getValue() {
        return val;
    }

    public void add(int n) {
        val += n;
        // WISHME: this would be a cool case for coalescing many updates.
        registerLocalOperation(new IntegerUpdate(n));
    }

    public void sub(int n) {
        add(-n);
    }

    /**
     * Applies donwstream add.
     * 
     * @param delta
     */
    protected void applyAdd(int delta) {
        val += delta;
    }

    @Override
    public IntegerCRDT copy() {
        return new IntegerCRDT(id, getTxnHandle(), getClock(), val);
    }
}
