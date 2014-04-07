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

import java.util.Set;

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTUpdate;

public class ReleaseOwnershipUpdate implements CRDTUpdate<SharedLockCRDT> {

    private String requesterId;
    private LockType type;
    private Set<TripleTimestamp> timestamps;
    private String parentId;

    // required for kryo
    public ReleaseOwnershipUpdate() {
    }

    public ReleaseOwnershipUpdate(String requesterId, String parentId, LockType type, Set<TripleTimestamp> timestamps) {
        this.requesterId = requesterId;
        this.parentId = parentId;
        this.type = type;
        this.timestamps = timestamps;
    }

    @Override
    public void applyTo(SharedLockCRDT crdt) {
        crdt.applyReleaseOwnership(this);
    }

    protected String getRequesterId() {
        return requesterId;
    }

    protected void setSiteId(String siteId) {
        this.requesterId = siteId;
    }

    protected LockType getType() {
        return type;
    }

    protected void setType(LockType type) {
        this.type = type;
    }

    public Set<TripleTimestamp> getTimestamps() {
        return timestamps;
    }

    protected void setTimestamp(Set<TripleTimestamp> timestamps) {
        this.timestamps = timestamps;
    }

    protected void setParent(String parentId) {
        this.parentId = parentId;
    }

    public String getParentId() {
        return parentId;
    }

}
