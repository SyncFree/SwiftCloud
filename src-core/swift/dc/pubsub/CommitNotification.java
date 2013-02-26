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
package swift.dc.pubsub;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.dc.ExecCRDTResult;

public class CommitNotification {

    List<ObjectSubscriptionInfo> info;
    public CommitUpdatesReply record;
    public CausalityClock dependencies, currVersion, stableVersion;

    // For Kryo, do not use...
    CommitNotification() {
    }

    public CommitNotification(ExecCRDTResult[] results, CommitUpdatesReply record) {
        this.record = record;
        this.info = new ArrayList<ObjectSubscriptionInfo>();

        for (ExecCRDTResult i : results)
            if (i.hasNotification())
                info.add(i.getInfo());
    }

    public Set<CRDTIdentifier> uids() {
        Set<CRDTIdentifier> res = new HashSet<CRDTIdentifier>();
        for (ObjectSubscriptionInfo i : info)
            res.add(i.getId());
        return res;
    }

    public Collection<ObjectSubscriptionInfo> info() {
        return info;
    }
}
