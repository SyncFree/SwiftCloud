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
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.IntegerUpdate;

public class IntegerTxnLocal extends BaseCRDTTxnLocal<IntegerVersioned> {
    private int val;

    public IntegerTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, IntegerVersioned creationState,
            int val) {
        super(id, txn, clock, creationState);
        this.val = val;
    }

    public Integer getValue() {
        return val;
    }

    public void add(int n) {
        val += n;
        TripleTimestamp ts = nextTimestamp();
        // TODO: this would be a cool case for coalescing many updates.
        registerLocalOperation(new IntegerUpdate(ts, n));
    }

    public void sub(int n) {
        add(-n);
    }

    @Override
    public Object executeQuery(CRDTQuery<IntegerVersioned> query) {
        return query.executeAt(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntegerTxnLocal)) {
            return false;
        }
        return ((IntegerTxnLocal) obj).val == this.val;
    }
}
