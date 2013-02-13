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
package swift.client.pubsub;

import java.util.HashSet;
import java.util.Set;

import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.crdt.CRDTIdentifier;
import swift.dc.pubsub.CommitNotification;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;

/**
 * Stub for the notification system. Currently only manages unsubscriptions...
 * 
 * @author smduarte
 * 
 */
public class ScoutPubSubService extends AbstractPubSub<CRDTIdentifier, CommitNotification> {

    final String clientId;
    final Endpoint surrogate;
    final RpcEndpoint endpoint;

    public ScoutPubSubService(String clientId, RpcEndpoint endpoint, Endpoint surrogate) {
        this.clientId = clientId;
        this.endpoint = endpoint;
        this.surrogate = surrogate;
    }

    @Override
    public void unsubscribe(CRDTIdentifier id, Handler<CRDTIdentifier, CommitNotification> handler) {
        synchronized (unsubscribed) {
            unsubscribed.add(id);
        }
        if (!unsubscriber.isScheduled())
            unsubscriber.reSchedule(0.1);
    }

    final Set<CRDTIdentifier> unsubscribed = new HashSet<CRDTIdentifier>();

    Task unsubscriber = new Task(0) {
        public void run() {
            Set<CRDTIdentifier> uids;
            synchronized (unsubscribed) {
                uids = new HashSet<CRDTIdentifier>(unsubscribed);
                unsubscribed.clear();
            }
            if (uids.size() > 0)
                endpoint.send(surrogate, new UnsubscribeUpdatesRequest(clientId, uids)).failed();
        }
    };
}
