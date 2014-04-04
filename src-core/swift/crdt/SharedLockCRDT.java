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
        this.type = LockType.WRITE_SHARED;
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
        if (type.equals(LockType.WRITE_EXCLUSIVE)) {
            return false;
        }
        if (parentId.equals(owner) && sharedOwners.isEmpty()) {
            return true;
        } else if (requestType.equals(this.type) && (sharedOwners.containsKey(parentId) || owner.equals(parentId))) {
            return true;
        }
        return false;
    }

    private boolean canReleaseOwnership(String requesterId, String parentId, LockType type) {
        if (requesterId.equals(parentId) && isOwner(requesterId, type)) {
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
            GetSharedLockUpdate op = new GetSharedLockUpdate(requesterId, parentId, type, nextTimestamp());
            applyGetOwnership(op);
            registerLocalOperation(op);
            return true;
        }
        return false;
    }

    public boolean releaseOwnership(String parentId, String requesterId, LockType type) {
        if (canReleaseOwnership(requesterId, parentId, type)) {
            Set<TripleTimestamp> removeTs = sharedOwners.get(requesterId);
            // Treat
            if (removeTs == null) {
                removeTs = new HashSet<TripleTimestamp>();
            }
            ReleaseSharedLockUpdate op = new ReleaseSharedLockUpdate(requesterId, parentId, type,
                    new HashSet<TripleTimestamp>(removeTs));
            applyReleaseOwnership(op);
            registerLocalOperation(op);
            return true;
        }
        return false;
    }

    protected void applyGetOwnership(GetSharedLockUpdate op) {
        // Check if the pre-condition of getOwnership is still valid
        if (!canGetOwnership(op.getRequesterId(), op.getParentId(), op.getType()))
            throw new IncompatibleLockException("Can't get ownership on downstream! op: " + op + " current: " + this);
        else if (op.getType().equals(LockType.WRITE_EXCLUSIVE)) {
            owner = op.getRequesterId();
        } else if (!op.getParentId().equals(owner) && op.getType().equals(LockType.READ_SHARED)
                || op.getType().equals(LockType.WRITE_SHARED)) {
            Set<TripleTimestamp> tsRequester = sharedOwners.get(op.getRequesterId());
            if (tsRequester == null) {
                tsRequester = new HashSet<TripleTimestamp>();
                sharedOwners.put(op.getRequesterId(), tsRequester);
            }
            tsRequester.add(op.getTimestamp());
        }
        type = op.getType();
    }

    public void applyReleaseOwnership(ReleaseSharedLockUpdate op) {
        // Check if everything is all right again
        if (!op.getType().equals(type))
            throw new IncompatibleLockException("Request release of an old lock" + op + " current: " + this);
        if (op.getType().equals(LockType.WRITE_EXCLUSIVE)) {
            type = LockType.WRITE_SHARED;
        } else {
            Set<TripleTimestamp> ts = sharedOwners.get(op.getRequesterId());
            ts.removeAll(op.getTimestamps());
            if (ts.size() == 0) {
                sharedOwners.remove(op.getRequesterId());
            }
        }
    }

}
