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

public class LWWRegisterUpdate<V> implements CRDTUpdate<LWWRegisterCRDT<V>> {
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

    public V getVal() {
        return this.val;
    }

    @Override
    public void applyTo(LWWRegisterCRDT<V> register) {
        register.applySet(registerTimestamp, tiebreakingTimestamp, val);
    }
}
