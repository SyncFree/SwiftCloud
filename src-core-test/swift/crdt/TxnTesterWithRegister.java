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
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.operations.CRDTObjectUpdatesGroup;

/*
 *  Pseudo Txn that linked to a unique CRDT.
 *  It applies the operations to the CRDT.
 */

public class TxnTesterWithRegister extends TxnTester {
    private CRDT<?> target = null;

    public <V extends CRDT<V>> TxnTesterWithRegister(String siteId, CausalityClock latestVersion, Timestamp ts,
            Timestamp globalTs, V target) {
        super(siteId, latestVersion, ts, globalTs);
        this.target = target;
    }

    public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
        if (target != null) {
            CRDTObjectUpdatesGroup<V> opGroup = (CRDTObjectUpdatesGroup<V>) objectOperations.get(target);
            if (opGroup == null) {
                opGroup = new CRDTObjectUpdatesGroup<V>(target.getUID(), tm, null, cc);
            }
            opGroup.append(op);
            objectOperations.put(target, opGroup);
        }
    }
}
