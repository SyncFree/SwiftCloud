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
package swift.pubsub;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.dc.ExecCRDTResult;
import swift.proto.ObjectUpdatesInfo;

public class CommitNotification {

    transient String clientId;
    Map<CRDTIdentifier, ObjectUpdatesInfo> info;
    public CausalityClock dependencies, currVersion, stableVersion;

    // For Kryo, do not use...
    CommitNotification() {
    }

    CommitNotification(CommitNotification other) {
        this.info = new HashMap<CRDTIdentifier, ObjectUpdatesInfo>(other.info);
        this.currVersion = other.currVersion;
        this.dependencies = other.dependencies;
        this.stableVersion = other.stableVersion;
    }

    public CommitNotification(String clientId, ExecCRDTResult[] results, CausalityClock snapshotClock,
            CausalityClock currVersion, CausalityClock stableVersion) {

        this.clientId = clientId;
        this.currVersion = currVersion;
        this.stableVersion = stableVersion;
        this.dependencies = snapshotClock;

        this.info = new HashMap<CRDTIdentifier, ObjectUpdatesInfo>();
        for (ExecCRDTResult i : results)
            info.put(i.getId(), i.getInfo());

    }

    public String getClientId() {
        return clientId;
    }

    public Set<CRDTIdentifier> uids() {
        return info.keySet();
    }

    public Collection<ObjectUpdatesInfo> info() {
        return info.values();
    }

    public CommitNotification clone(Set<CRDTIdentifier> subSet) {
        CommitNotification res = new CommitNotification(this);
        res.info.keySet().retainAll(subSet);
        return res;
    }
}
