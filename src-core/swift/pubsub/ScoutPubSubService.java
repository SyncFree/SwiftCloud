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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import swift.crdt.CRDTIdentifier;
import swift.proto.SwiftProtocolHandler;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import swift.proto.UpdatesNotification;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;
import sys.utils.FifoQueue;

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
    final Set<CRDTIdentifier> subscriptions = new ConcurrentSkipListSet<CRDTIdentifier>();

    final FifoQueue<UpdatesNotification> fifoQueue;
    final Set<CRDTIdentifier> removals = new ConcurrentSkipListSet<CRDTIdentifier>();

    final Map<Long, UnsubscribeUpdatesRequest> updates = new ConcurrentHashMap<Long, UnsubscribeUpdatesRequest>();
    RpcHandler replyHandler;

    List<Integer> gots = new ArrayList<Integer>();
    boolean bound2dc = false;

    public ScoutPubSubService(final String clientId, RpcEndpoint endpoint, Endpoint surrogate) {
        this.clientId = clientId;
        this.endpoint = endpoint;
        this.surrogate = surrogate;

        this.replyHandler = new SwiftProtocolHandler() {
            protected void onReceive(RpcHandle conn, UnsubscribeUpdatesReply ack) {
                bound2dc = true;
                // System.err.println(ack.getId());
            }

            protected void onReceive(RpcHandle conn, UpdatesNotification request) {
                fifoQueue.offer(request.seqN(), request);
            }
        };

        this.fifoQueue = new FifoQueue<UpdatesNotification>() {
            public void process(UpdatesNotification p) {
                gots.add(p.seqN());
                // System.err.println(gots);
                for (CommitNotification r : p.getRecords())
                    ScoutPubSubService.this.notify(r.info.keySet(), r);
            }
        };
    }

    public boolean isSubscribed(CRDTIdentifier id) {
        return subscriptions.contains(id);
    }

    @Override
    public void unsubscribe(CRDTIdentifier id, Handler<CRDTIdentifier, CommitNotification> handler) {
        if (subscriptions.remove(id)) {
            removals.add(id);
            if (!updater.isScheduled())
                updater.reSchedule(0.1);
        }
    }

    @Override
    public void subscribe(CRDTIdentifier id, Handler<CRDTIdentifier, CommitNotification> handler) {
        subscriptions.add(id);
    }

    Task updater = new Task(3) {
        public void run() {
            if (removals.size() > 0 || !bound2dc) {
                UnsubscribeUpdatesRequest req = new UnsubscribeUpdatesRequest(0L, clientId, removals);
                endpoint.send(surrogate, req, replyHandler, 0);
                removals.clear();
            }
            if (!bound2dc)
                reSchedule(1);
        }
    };
}
