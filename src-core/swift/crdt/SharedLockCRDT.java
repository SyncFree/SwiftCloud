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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

public class SharedLockCRDT extends BaseCRDT<SharedLockCRDT> {

    private String owner;
    private LockType type;
    private Map<String, Set<TripleTimestamp>> sharedOwners;
    private Map<String, Integer> active;

    // TODO: Does it need to track WRITE_EXCLUSIVE versions? apparently not.

    // For kryo
    public SharedLockCRDT() {
    }

    public SharedLockCRDT(CRDTIdentifier id, String owner) {
        super(id);
        this.owner = owner;
        this.type = LockType.ALLOW;
        this.sharedOwners = new HashMap<String, Set<TripleTimestamp>>();
        this.active = new HashMap<String, Integer>();
    }

    public SharedLockCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, String owner, LockType type,
            Map<String, Set<TripleTimestamp>> sharedOwners, Map<String, Integer> active) {
        super(id, txn, clock);
        this.owner = owner;
        this.type = type;
        this.sharedOwners = sharedOwners;
        this.active = active;
    }

    @Override
    public LockType getValue() {
        return type;
    }

    @Override
    public SharedLockCRDT copy() {
        Map<String, Set<TripleTimestamp>> sharedOwnersCopy = new HashMap<String, Set<TripleTimestamp>>();
        for (Entry<String, Set<TripleTimestamp>> entry : sharedOwners.entrySet()) {
            sharedOwnersCopy.put(entry.getKey(), new HashSet<TripleTimestamp>(sharedOwnersCopy.get(entry.getKey())));
        }
        return new SharedLockCRDT(id, txn, clock, owner, type, sharedOwnersCopy, new HashMap<String, Integer>(active));
    }

    /**
     * Returns true if the siteId is able to get the lock on requestType
     */
    private boolean canGetOwnership(String requesterId, String parentId, LockType requestType) {
        if (type.equals(LockType.EXCLUSIVE_ALLOW)) {
            return false;
        }
        if (parentId.equals(owner) && sharedOwners.isEmpty()) {
            return true;
        } else if (requestType.equals(this.type) && (sharedOwners.containsKey(parentId) || owner.equals(parentId))) {
            return true;
        }
        return false;
    }

    private boolean canReleaseOwnership(String parentId, LockType type) {
        if (active.get(parentId) != null) {
            return false;
        } else if (isOwner(parentId, type)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isOwner(String requesterId, LockType lock) {
        if (requesterId.equals(owner) || sharedOwners.containsKey(requesterId)) {
            return lock.equals(this.type);
        }
        return false;
    }

    public boolean getOwnership(String parentId, String requesterId, LockType type) {
        if (canGetOwnership(requesterId, parentId, type)) {
            GetOwnershipUpdate op = new GetOwnershipUpdate(requesterId, parentId, type, nextTimestamp());
            applyGetOwnership(op);
            registerLocalOperation(op);
            return true;
        }
        return false;
    }

    public boolean releaseOwnership(String ownerId, LockType type) {
        if (canReleaseOwnership(ownerId, type)) {
            Set<TripleTimestamp> removeTs = sharedOwners.get(ownerId);
            // Treat
            if (removeTs == null) {
                removeTs = new HashSet<TripleTimestamp>();
            }
            ReleaseOwnershipUpdate op = new ReleaseOwnershipUpdate(ownerId, ownerId, type,
                    new HashSet<TripleTimestamp>(removeTs));
            applyReleaseOwnership(op);
            registerLocalOperation(op);
            return true;
        }
        return false;
    }

    public boolean lock(String ownerId, LockType type) {
        if (isOwner(ownerId, type)
                && (!type.equals(LockType.EXCLUSIVE_ALLOW) || (type.equals(LockType.EXCLUSIVE_ALLOW) && active
                        .get(ownerId) == null))) {
            AcquireLockUpdate op = new AcquireLockUpdate(ownerId, type);
            applyAcquireLock(op);
            registerLocalOperation(op);
            return true;
        } else if (canGetOwnership(ownerId, ownerId, type)) {
            getOwnership(ownerId, ownerId, type);
            return lock(ownerId, type);
        }
        return false;
    }

    public boolean unlock(String ownerId, LockType type) {
        if (isOwner(ownerId, type) && active.containsKey(ownerId)) {
            ReleaseLockUpdate op = new ReleaseLockUpdate(ownerId, type);
            applyReleaseLock(op);
            registerLocalOperation(op);
            return true;
        }
        return false;
    }

    protected void applyGetOwnership(GetOwnershipUpdate op) {
        // Check if the pre-condition of getOwnership is still valid
        if (!canGetOwnership(op.getRequesterId(), op.getParentId(), op.getType()))
            throw new IncompatibleLockException("Can't get ownership on downstream! op: " + op + " current: " + this);
        else if (op.getType().equals(LockType.EXCLUSIVE_ALLOW)) {
            owner = op.getRequesterId();
        } else if (op.getType().equals(LockType.FORBID) || op.getType().equals(LockType.ALLOW)) {
            Set<TripleTimestamp> tsRequester = sharedOwners.get(op.getRequesterId());
            if (tsRequester == null) {
                tsRequester = new HashSet<TripleTimestamp>();
                sharedOwners.put(op.getRequesterId(), tsRequester);
            }
            tsRequester.add(op.getTimestamp());
        }
        type = op.getType();
    }

    public void applyReleaseOwnership(ReleaseOwnershipUpdate op) {
        // Check if everything is all right again
        if (!op.getType().equals(type))
            throw new IncompatibleLockException("Request to release ownership for a different type" + op + " current: "
                    + this);
        if (op.getType().equals(LockType.EXCLUSIVE_ALLOW)) {
            type = LockType.ALLOW;
        } else {
            Set<TripleTimestamp> ts = sharedOwners.get(op.getRequesterId());
            ts.removeAll(op.getTimestamps());
            if (ts.size() == 0) {
                sharedOwners.remove(op.getRequesterId());
            }
        }
    }

    public void applyAcquireLock(AcquireLockUpdate op) {
        if (!op.getType().equals(type))
            throw new IncompatibleLockException("Request to use a lock with different type" + op + " current: " + this);
        Integer activeUsers = active.get(op.getOwnerId());
        if (activeUsers == null)
            active.put(op.getOwnerId(), 1);
        else
            active.put(op.getOwnerId(), activeUsers + 1);
    }

    public void applyReleaseLock(ReleaseLockUpdate op) {
        if (!op.getType().equals(type))
            throw new IncompatibleLockException("Request to release a lock with different type" + op + " current: "
                    + this);
        Integer activeUsers = active.get(op.getOwnerId());
        activeUsers = activeUsers - 1;
        if (activeUsers > 0) {
            active.put(op.getOwnerId(), activeUsers);
        } else {
            active.remove(op.getOwnerId());
        }
    }

}
