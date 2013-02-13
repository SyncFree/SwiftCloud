package swift.dc.pubsub;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import swift.crdt.CRDTIdentifier;
import sys.pubsub.impl.AbstractPubSub;

public class DcPubSubService extends AbstractPubSub<CRDTIdentifier, CommitNotification> {

    Executor executor = Executors.newCachedThreadPool();
    Map<CRDTIdentifier, Set<Handler<CRDTIdentifier, CommitNotification>>> subscribers;

    public DcPubSubService() {
        subscribers = new HashMap<CRDTIdentifier, Set<Handler<CRDTIdentifier, CommitNotification>>>();
    }

    @Override
    synchronized public void publish(final Set<CRDTIdentifier> uids, final CommitNotification info) {
        // TODO: publish to other surrogates...
        final Set<Handler<CRDTIdentifier, CommitNotification>> targets = new HashSet<Handler<CRDTIdentifier, CommitNotification>>();
        for (CRDTIdentifier i : uids)
            targets.addAll(subscribers(i, false));
        // System.err.println("TARGETS FOR: " + uids + "----->" +
        // targets.size());
        executor.execute(new Runnable() {
            public void run() {
                for (Handler<CRDTIdentifier, CommitNotification> i : targets)
                    i.notify(uids, info);
            }
        });
    }

    @Override
    synchronized public void subscribe(final CRDTIdentifier id, Handler<CRDTIdentifier, CommitNotification> handler) {
        Set<Handler<CRDTIdentifier, CommitNotification>> set = subscribers(id, false);
        if (set.isEmpty()) {
            // TODO: Refresh subscriptions on Decentralized Version...
        }
        set.add(handler);
    }

    @Override
    synchronized public void unsubscribe(CRDTIdentifier id, Handler<CRDTIdentifier, CommitNotification> handler) {
        Set<Handler<CRDTIdentifier, CommitNotification>> set = subscribers(id, false);
        set.remove(handler);
        if (set.isEmpty()) {
            // TODO: Refresh subscriptions on Decentralized Version...
            // note: unsubscribe can be called several times, need some way to
            // coalesce calls...maybe with a task to fire the update...
        }
    }

    private Set<Handler<CRDTIdentifier, CommitNotification>> subscribers(CRDTIdentifier id, boolean clone) {
        Set<Handler<CRDTIdentifier, CommitNotification>> res = subscribers.get(id);
        if (res == null)
            subscribers.put(id, res = new HashSet<Handler<CRDTIdentifier, CommitNotification>>());

        return clone ? new HashSet<Handler<CRDTIdentifier, CommitNotification>>(res) : res;
    }
}
