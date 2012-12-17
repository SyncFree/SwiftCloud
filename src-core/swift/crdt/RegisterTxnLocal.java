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
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnGetterSetter;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.RegisterUpdate;

public class RegisterTxnLocal<V extends Copyable> extends BaseCRDTTxnLocal<RegisterVersioned<V>> implements
        TxnGetterSetter<V> {
    private V val;
    private long nextLamportClock;

    public RegisterTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, RegisterVersioned<V> creationState,
            V val, long nextLamportClock) {
        super(id, txn, clock, creationState);
        this.val = val;
        this.nextLamportClock = nextLamportClock;
    }

    @Override
    public void set(V v) {
        val = v;
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new RegisterUpdate<V>(ts, nextLamportClock++, v));
    }

    @Override
    public V getValue() {
        return val;
    }

    @Override
    public Object executeQuery(CRDTQuery<RegisterVersioned<V>> query) {
        return query.executeAt(this);
    }
}
