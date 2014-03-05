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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Publisher;
import sys.pubsub.PubSub.Subscriber;

public abstract class AbstractPubSub<T> implements PubSub<T>, Subscriber<T>, Publisher<T, Notifyable<T>> {

    final Map<Object, Set<Subscriber<T>>> subscribers;

    protected AbstractPubSub() {
        subscribers = new HashMap<Object, Set<Subscriber<T>>>();
    }

    @Override
    public void publish(Notifyable<T> info) {
        info.notifyTo(this);
    }

    @Override
    public void onNotification(Notifyable<T> info) {
        Thread.dumpStack();
    }

    @Override
    synchronized public boolean subscribe(T key, Subscriber<T> subscriber) {
        Set<Subscriber<T>> res = subscribers.get(key);
        if (res == null)
            subscribers.put(key, res = new HashSet<Subscriber<T>>());

        return res.add(subscriber);
    }

    @Override
    synchronized public boolean unsubscribe(T key, Subscriber<T> subscriber) {
        Set<Subscriber<T>> ss = subscribers.get(key);
        return ss != null && ss.remove(subscriber) && ss.isEmpty();
    }

    @Override
    synchronized public boolean subscribe(Set<T> keys, Subscriber<T> Subscriber) {
        boolean changed = false;
        for (T i : keys)
            changed |= subscribe(i, Subscriber);

        return changed;
    }

    @Override
    synchronized public boolean unsubscribe(Set<T> keys, Subscriber<T> subscriber) {
        boolean changed = false;
        for (T i : keys)
            changed |= unsubscribe(i, subscriber);
        return changed;
    }

    @Override
    public synchronized Set<Subscriber<T>> subscribers(T key, boolean clone) {
        Set<Subscriber<T>> res = subscribers.get(key);
        res = res != null ? res : Collections.unmodifiableSet(Collections.<Subscriber<T>> emptySet());
        return clone ? new HashSet<Subscriber<T>>(res) : res;
    }

    @Override
    public synchronized Set<Subscriber<T>> subscribers(Set<T> keys) {
        Set<Subscriber<T>> res = new HashSet<Subscriber<T>>();
        for (T i : keys)
            res.addAll(subscribers(i, false));
        return res;
    }

    @Override
    public boolean isSubscribed(T key, Subscriber<T> handler) {
        return subscribers(key, true).contains(handler);
    }

    @Override
    synchronized public boolean isSubscribed(Object key) {
        return subscribers.containsKey(key);
    }
}
