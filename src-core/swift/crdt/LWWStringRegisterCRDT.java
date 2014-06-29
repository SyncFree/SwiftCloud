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
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * LWW register CRDT optimized for storing strings.
 * 
 * @author mzawirsk
 */
public class LWWStringRegisterCRDT extends AbstractLWWRegisterCRDT<String, LWWStringRegisterCRDT> {
    // Kryo
    public LWWStringRegisterCRDT() {
    }

    public LWWStringRegisterCRDT(CRDTIdentifier uid) {
        super(uid);
    }

    private LWWStringRegisterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            LWWRegisterUpdate<String, LWWStringRegisterCRDT> lastUpdate) {
        super(id, txn, clock, lastUpdate);
    }

    @Override
    protected LWWStringRegisterUpdate generateUpdate(long registerTimestamp, TripleTimestamp tiebreakingTimestamp,
            String value) {
        return new LWWStringRegisterUpdate(registerTimestamp, tiebreakingTimestamp, value);
    }

    @Override
    public LWWStringRegisterCRDT copy() {
        return new LWWStringRegisterCRDT(id, txn, clock, lastUpdate);
    }
}
