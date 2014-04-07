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

public class GetOwnershipUpdate implements CRDTUpdate<SharedLockCRDT> {

    private String requesterId;
    private LockType type;
    private TripleTimestamp timestamp;
    private String parentId;

    // required for kryo
    public GetOwnershipUpdate() {
    }

    public GetOwnershipUpdate(String requesterId, String parentId, LockType type, TripleTimestamp timestamp) {
        this.requesterId = requesterId;
        this.parentId = parentId;
        this.type = type;
        this.timestamp = timestamp;
    }

    @Override
    public void applyTo(SharedLockCRDT crdt) {
        crdt.applyGetOwnership(this);
    }

    protected String getRequesterId() {
        return requesterId;
    }

    protected void setRequesterId(String requesterId) {
        this.requesterId = requesterId;
    }

    protected LockType getType() {
        return type;
    }

    protected void setType(LockType type) {
        this.type = type;
    }

    public TripleTimestamp getTimestamp() {
        return timestamp;
    }

    protected void setTimestamp(TripleTimestamp timestamp) {
        this.timestamp = timestamp;
    }

    protected void setParent(String parentId) {
        this.parentId = parentId;
    }

    public String getParentId() {
        return parentId;
    }

}
