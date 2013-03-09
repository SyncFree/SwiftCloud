package swift.pubsub;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import swift.crdt.CRDTIdentifier;
import sys.pubsub.impl.AbstractPubSub;
import sys.scheduler.Task;

public class DcPubSubService extends AbstractPubSub<CRDTIdentifier, CommitNotification> {

    Executor executor = Executors.newCachedThreadPool();
    Set<CRDTIdentifier> subscription = new HashSet<CRDTIdentifier>();

    Map<CRDTIdentifier, Set<Handler<CRDTIdentifier, CommitNotification>>> subscribers;

    public DcPubSubService() {
        subscribers = new ConcurrentHashMap<CRDTIdentifier, Set<Handler<CRDTIdentifier, CommitNotification>>>();
    }

    @Override
    synchronized public void publish(final Set<CRDTIdentifier> uids, final CommitNotification info) {
        // TODO: publish to other surrogates...

        final Set<Handler<CRDTIdentifier, CommitNotification>> targets = new HashSet<Handler<CRDTIdentifier, CommitNotification>>();
        for (CRDTIdentifier i : uids)
            targets.addAll(subscribers(i, false));

        executor.execute(new Runnable() {
            public void run() {
                for (Handler<CRDTIdentifier, CommitNotification> i : targets)
                    i.notify(uids, info);
            }
        });
    }

    @Override
    synchronized public void subscribe(CRDTIdentifier id, Handler<CRDTIdentifier, CommitNotification> handler) {
        if (subscribers(id, false).add(handler))
            updater.reSchedule(0.01);
    }

    // @Override
    // synchronized public void subscribe(Set<CRDTIdentifier> uids,
    // Handler<CRDTIdentifier, CommitNotification> handler) {
    // int oldSize = subscription.size();
    // for (CRDTIdentifier i : uids) {
    // subscribers(i, false).add(handler);
    // subscription.add(i);
    // }
    // if (subscription.size() != oldSize)
    // updater.reSchedule(0.01);
    // }

    @Override
    synchronized public void unsubscribe(Set<CRDTIdentifier> uids, Handler<CRDTIdentifier, CommitNotification> handler) {
        int oldSize = subscription.size();
        for (CRDTIdentifier i : uids) {
            Set<?> subscribers = subscribers(i, false);
            subscribers.remove(handler);
        }
        if (subscription.size() != oldSize)
            updater.reSchedule(0.01);
    }

    synchronized private Set<Handler<CRDTIdentifier, CommitNotification>> subscribers(CRDTIdentifier id, boolean clone) {
        Set<Handler<CRDTIdentifier, CommitNotification>> res = subscribers.get(id);
        if (res == null)
            subscribers.put(id, res = new HashSet<Handler<CRDTIdentifier, CommitNotification>>());

        return clone ? new HashSet<Handler<CRDTIdentifier, CommitNotification>>(res) : res;
    }

    Task updater = new Task(0) {
        public void run() {
            // System.err.println("-------NOTIFY OTHER SURROGATES OF NEW SUBSCRIPTION:"
            // + subscription.size());
        }
    };
}
