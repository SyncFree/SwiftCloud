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

import swift.application.social.User;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * LWW register CRDT optimized for storing SwiftSocial Users data. WARNING: When
 * assigning a value to a register, make sure that the User value is immutable
 * (e.g., a private copy).
 * 
 * @author mzawirsk
 */
// TODO: it used to be that Register's content was copyable, but nothing was
// copied actually in the implementation? Current solution is consistent with
// Set CRDTs, but perhaps suboptimal - we could enforce clonability of V and
// clone values just after the API calls.
public class LWWUserRegisterCRDT extends AbstractLWWRegisterCRDT<User, LWWUserRegisterCRDT> {
    // Kryo
    public LWWUserRegisterCRDT() {
    }

    public LWWUserRegisterCRDT(CRDTIdentifier uid) {
        super(uid);
    }

    private LWWUserRegisterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock,
            LWWRegisterUpdate<User, LWWUserRegisterCRDT> lastUpdate) {
        super(id, txn, clock, lastUpdate);
    }

    @Override
    protected LWWRegisterUpdate<User, LWWUserRegisterCRDT> generateUpdate(long registerTimestamp,
            TripleTimestamp tiebreakingTimestamp, User value) {
        return new LWWUserRegisterUpdate(registerTimestamp, tiebreakingTimestamp, value);
    }

    @Override
    public LWWUserRegisterCRDT copy() {
        return new LWWUserRegisterCRDT(id, txn, clock, lastUpdate);
    }
}
