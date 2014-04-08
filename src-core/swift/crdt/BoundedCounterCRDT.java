package swift.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

public abstract class BoundedCounterCRDT<T extends BoundedCounterCRDT<T>> extends BaseCRDT<T> {

    protected Map<String, Map<String, Integer>> permissions;
    protected Map<String, Integer> delta;
    protected int initVal, val;

    public BoundedCounterCRDT() {
        super();
    }

    public BoundedCounterCRDT(CRDTIdentifier id) {
        super(id);
    }

    public BoundedCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock) {
        super(id, txn, clock);
    }

    public BoundedCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int initVal,
            Map<String, Map<String, Integer>> permissions, Map<String, Integer> decrements) {
        super(id, txn, clock);
        this.permissions = permissions;
        this.delta = decrements;
        this.initVal = initVal;
        this.val = initVal + totalAvailable() - totalSpent();
    }

    public BoundedCounterCRDT(CRDTIdentifier id, int initVal) {
        super(id);
        this.initVal = initVal;
        this.val = initVal;
        this.permissions = new HashMap<String, Map<String, Integer>>();
        this.delta = new HashMap<String, Integer>();

    }

    @Override
    public Integer getValue() {
        return val;
    }

    protected int totalSpent() {
        int totalSpent = 0;
        for (Integer spentSiteId : delta.values()) {
            totalSpent += spentSiteId;
        }
        return totalSpent;
    }

    protected int totalAvailable() {
        int totalAvailable = 0;
        for (Entry<String, Map<String, Integer>> sitePermissions : permissions.entrySet()) {
            totalAvailable += sitePermissions.getValue().get(sitePermissions.getValue());
        }
        return totalAvailable;
    }

    protected void checkExistsPermissionPair(String leftId, String rightId) {
        if (!permissions.containsKey(leftId)) {
            HashMap<String, Integer> sitePerm = new HashMap<String, Integer>();
            sitePerm.put(rightId, 0);
            if (leftId.equals(rightId)) {
                delta.put(leftId, 0);
            }
            permissions.put(leftId, sitePerm);
        }

        if (!permissions.get(leftId).containsKey(rightId)) {
            permissions.get(leftId).put(rightId, 0);
        }
    }

    protected int availableSiteId(String siteId) {
        int given = 0;
        int received = 0;
        int spent = 0;

        checkExistsPermissionPair(siteId, siteId);
        spent = delta.get(siteId);
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

    public void applyTransfer(BoundedCounterTransfer<T> transferUpdate) {
        checkExistsPermissionPair(transferUpdate.getOriginId(), transferUpdate.getTargetId());
        Map<String, Integer> targetPermissions = permissions.get(transferUpdate.getOriginId());
        targetPermissions.put(transferUpdate.getTargetId(), targetPermissions.get(transferUpdate.getTargetId())
                + transferUpdate.getAmount());
    }

    public abstract boolean decrement(int amount, String siteId);

    public abstract boolean increment(int amount, String siteId);

    public abstract boolean transfer(int amount, String originId, String targetId);

    protected abstract void applyInc(BoundedCounterIncrement<T> incUpdate);

    protected abstract void applyDec(BoundedCounterDecrement<T> decUpdate);

}
