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

public class LowerBoundCounterCRDT extends BoundedCounterCRDT<LowerBoundCounterCRDT> {

    private Map<String, Map<String, Integer>> permissions;
    private Map<String, Integer> decrements;
    private int initVal, val;

    // For kryo
    public LowerBoundCounterCRDT() {
    }

    public LowerBoundCounterCRDT(CRDTIdentifier id) {
        this(id, 0);
    }

    public LowerBoundCounterCRDT(CRDTIdentifier id, int initVal) {
        super(id);
        this.initVal = initVal;
        this.val = initVal;
        this.permissions = new HashMap<String, Map<String, Integer>>();
        this.decrements = new HashMap<String, Integer>();

    }

    public LowerBoundCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int initVal,
            Map<String, Map<String, Integer>> permissions, Map<String, Integer> decrements) {
        super(id, txn, clock);
        this.permissions = permissions;
        this.decrements = decrements;
        this.initVal = initVal;
        this.val = initVal + totalAvailable() - totalSpent();
    }

    @Override
    public Integer getValue() {
        return val;
    }

    @Override
    public LowerBoundCounterCRDT copy() {
        Map<String, Map<String, Integer>> permCopy = new HashMap<String, Map<String, Integer>>(permissions);
        for (Entry<String, Map<String, Integer>> entry : permissions.entrySet()) {
            permCopy.put(entry.getKey(), new HashMap<String, Integer>(entry.getValue()));
        }
        return new LowerBoundCounterCRDT(id, txn, clock, initVal, permCopy, new HashMap<String, Integer>(decrements));
    }

    private int totalSpent() {
        int totalSpent = 0;
        for (Integer spentSiteId : decrements.values()) {
            totalSpent += spentSiteId;
        }
        return totalSpent;
    }

    private int totalAvailable() {
        int totalAvailable = 0;
        for (Entry<String, Map<String, Integer>> sitePermissions : permissions.entrySet()) {
            totalAvailable += sitePermissions.getValue().get(sitePermissions.getValue());
        }
        return totalAvailable;
    }

    private int availableSiteId(String siteId) {
        int given = 0;
        int received = 0;
        int spent = 0;

        checkExistsPermissionPair(siteId, siteId);
        spent = decrements.get(siteId);
        Map<String, Integer> sitePermissions = permissions.get(siteId);
        for (Entry<String, Integer> p : sitePermissions.entrySet()) {
            if (!p.getKey().equals(siteId)) {
                given += p.getValue();
            }
        }
        for (Entry<String, Map<String, Integer>> allPermissions : permissions.entrySet()) {
            Integer receivedPermissions = allPermissions.getValue().get(siteId);
            if (receivedPermissions != null) {
                received += receivedPermissions;
            }
        }
        return received - given - spent;
    }

    public boolean increment(int amount, String siteId) {
        BoundedCounterIncrement<LowerBoundCounterCRDT> update = new BoundedCounterIncrement<LowerBoundCounterCRDT>(
                siteId, amount);
        applyInc(update);
        registerLocalOperation(update);
        return true;
    }

    public boolean decrement(int amount, String siteId) {
        if (availableSiteId(siteId) >= amount) {
            BoundedCounterDecrement<LowerBoundCounterCRDT> update = new BoundedCounterDecrement<LowerBoundCounterCRDT>(
                    siteId, amount);
            applyDec(update);
            registerLocalOperation(update);
            return true;
        }
        return false;
    }

    private void checkExistsPermissionPair(String leftId, String rightId) {
        if (!permissions.containsKey(leftId)) {
            HashMap<String, Integer> sitePerm = new HashMap<String, Integer>();
            sitePerm.put(rightId, 0);
            if (leftId.equals(rightId)) {
                decrements.put(leftId, 0);
            }
            permissions.put(leftId, sitePerm);
        }

        if (!permissions.get(leftId).containsKey(rightId)) {
            permissions.get(leftId).put(rightId, 0);
        }
    }

    public boolean transfer(int amount, String originId, String targetId) {
        if (availableSiteId(originId) >= amount) {
            BoundedCounterTransfer<LowerBoundCounterCRDT> update = new BoundedCounterTransfer<LowerBoundCounterCRDT>(
                    originId, targetId, amount);
            applyTransfer(update);
            registerLocalOperation(update);
            return true;
        }
        return false;
    }

    public void applyDec(BoundedCounterDecrement<LowerBoundCounterCRDT> decUpdate) {
        checkExistsPermissionPair(decUpdate.getSiteId(), decUpdate.getSiteId());
        decrements.put(decUpdate.getSiteId(), decUpdate.getAmount());
        val -= decUpdate.getAmount();

    }

    public void applyInc(BoundedCounterIncrement<LowerBoundCounterCRDT> incUpdate) {
        checkExistsPermissionPair(incUpdate.getSiteId(), incUpdate.getSiteId());
        Map<String, Integer> sitePermissions = permissions.get(incUpdate.getSiteId());
        sitePermissions.put(incUpdate.getSiteId(), sitePermissions.get(incUpdate.getSiteId()) + incUpdate.getAmount());
        val += incUpdate.getAmount();
    }

    public void applyTransfer(BoundedCounterTransfer<LowerBoundCounterCRDT> transferUpdate) {
        checkExistsPermissionPair(transferUpdate.getOriginId(), transferUpdate.getTargetId());
        Map<String, Integer> targetPermissions = permissions.get(transferUpdate.getOriginId());
        targetPermissions.put(transferUpdate.getTargetId(), targetPermissions.get(transferUpdate.getTargetId())
                + transferUpdate.getAmount());
    }

}
