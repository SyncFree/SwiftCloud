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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

public class UpperBoundCounterCRDT extends BoundedCounterCRDT<UpperBoundCounterCRDT> {

    // For kryo
    public UpperBoundCounterCRDT() {
    }

    public UpperBoundCounterCRDT(CRDTIdentifier id) {
        this(id, 0);
    }

    public UpperBoundCounterCRDT(CRDTIdentifier id, int initVal) {
        super(id, initVal);
    }

    public UpperBoundCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int initVal,
            Map<String, Map<String, Integer>> permissions, Map<String, Integer> decrements) {
        super(id, txn, clock, initVal, permissions, decrements);
    }

    @Override
    public UpperBoundCounterCRDT copy() {
        Map<String, Map<String, Integer>> permCopy = new HashMap<String, Map<String, Integer>>(permissions);
        for (Entry<String, Map<String, Integer>> entry : permissions.entrySet()) {
            permCopy.put(entry.getKey(), new HashMap<String, Integer>(entry.getValue()));
        }
        return new UpperBoundCounterCRDT(id, txn, clock, initVal, permCopy, new HashMap<String, Integer>(delta));
    }

    public boolean increment(int amount, String siteId) {
        if (availableSiteId(siteId) >= amount) {
            BoundedCounterIncrement<UpperBoundCounterCRDT> update = new BoundedCounterIncrement<UpperBoundCounterCRDT>(
                    siteId, amount);
            applyInc(update);
            registerLocalOperation(update);
            return true;
        }
        return false;
    }

    public boolean decrement(int amount, String siteId) {
        BoundedCounterDecrement<UpperBoundCounterCRDT> update = new BoundedCounterDecrement<UpperBoundCounterCRDT>(
                siteId, amount);
        applyDec(update);
        registerLocalOperation(update);
        return true;
    }

    public boolean transfer(int amount, String originId, String targetId) {
        if (availableSiteId(originId) >= amount) {
            BoundedCounterTransfer<UpperBoundCounterCRDT> update = new BoundedCounterTransfer<UpperBoundCounterCRDT>(
                    originId, targetId, amount);
            applyTransfer(update);
            registerLocalOperation(update);
            return true;
        }
        return false;
    }

    protected void applyDec(BoundedCounterDecrement<UpperBoundCounterCRDT> decUpdate) {
        checkExistsPermissionPair(decUpdate.getSiteId(), decUpdate.getSiteId());
        Map<String, Integer> sitePermissions = permissions.get(decUpdate.getSiteId());
        sitePermissions.put(decUpdate.getSiteId(), sitePermissions.get(decUpdate.getSiteId()) + decUpdate.getAmount());
        val -= decUpdate.getAmount();
    }

    protected void applyInc(BoundedCounterIncrement<UpperBoundCounterCRDT> incUpdate) {
        checkExistsPermissionPair(incUpdate.getSiteId(), incUpdate.getSiteId());
        delta.put(incUpdate.getSiteId(), incUpdate.getAmount());
        val += incUpdate.getAmount();
    }

}
