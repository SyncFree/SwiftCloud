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
package sys.pubsub.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Publisher;
import sys.utils.ConcurrentHashSet;
import sys.utils.Threading;

public abstract class AbstractPubSub<T> extends AbstractSubscriber<T> implements PubSub<T>,
        Publisher<T, Notifyable<T>>, Runnable {

    protected final Map<T, Set<Subscriber<T>>> subscribers;

    protected final LinkedList<Notification<T>> notificationQueue = new LinkedList<Notification<T>>();

    protected AbstractPubSub(String id) {
        super(id);
        this.subscribers = new ConcurrentHashMap<T, Set<Subscriber<T>>>();
        Threading.newThread(true, this).start();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    synchronized public void publish(Notifyable<T> info) {
        Collection<Subscriber<T>> set = info.key() == null ? subscribers(info.keys()) : subscribers(info.key(), false);
        if (set.size() > 0)
            synchronized (notificationQueue) {
                for (Subscriber<T> i : set) {
                    notificationQueue.addLast(new Notification<>(info.clone(i.nextSeqN()), i));
                }
                Threading.notifyAllOn(notificationQueue);
            }
    }

    public void run() {
        for (;;) {
            Notification<T> head;
            synchronized (notificationQueue) {
                while (notificationQueue.isEmpty()) {
                    Threading.waitOn(notificationQueue);
                }
                head = notificationQueue.removeFirst();
            }
            try {
                head.e.notifyTo(head.s);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    @Override
    public boolean subscribe(T key, Subscriber<T> subscriber) {
        Set<Subscriber<T>> set = subscribers.get(key), nset;
        if (set == null) {
            set = subscribers.put(key, nset = new ConcurrentHashSet<Subscriber<T>>());
            if (set == null)
                set = nset;
        }
        return set.add(subscriber);
    }

    @Override
    public boolean unsubscribe(T key, Subscriber<T> subscriber) {
        Set<Subscriber<T>> set = subscribers.get(key);
        return set != null && set.remove(subscriber) && set.isEmpty();
    }

    @Override
    public boolean subscribe(Set<T> keys, Subscriber<T> Subscriber) {
        boolean changed = false;
        for (T i : keys)
            changed |= subscribe(i, Subscriber);

        return changed;
    }

    @Override
    public boolean unsubscribe(Set<T> keys, Subscriber<T> subscriber) {
        boolean changed = false;
        for (T i : keys)
            changed |= unsubscribe(i, subscriber);
        return changed;
    }

    @Override
    public Set<Subscriber<T>> subscribers(T key, boolean clone) {
        Set<Subscriber<T>> set = subscribers.get(key);
        set = set != null ? set : Collections.<Subscriber<T>> emptySet();
        return Collections.unmodifiableSet(clone ? new HashSet<Subscriber<T>>(set) : set);
    }

    @Override
    public Set<Subscriber<T>> subscribers(Set<T> keys) {
        Set<Subscriber<T>> res = new HashSet<Subscriber<T>>();
        for (T i : keys)
            res.addAll(subscribers(i, false));
        return res;
    }

    @Override
    public boolean isSubscribed(T key, Subscriber<T> handler) {
        return subscribers(key, false).contains(handler);
    }

    @Override
    public boolean isSubscribed(T key) {
        return subscribers.containsKey(key);
    }

    static class Notification<T> {
        final Notifyable<T> e;
        final Subscriber<T> s;

        Notification(Notifyable<T> e, Subscriber<T> s) {
            this.e = e;
            this.s = s;
        }

        public String toString() {
            return s.getClass() + "/" + e.getClass();
        }
    }
}
