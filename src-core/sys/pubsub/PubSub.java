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
package sys.pubsub;

import java.util.Set;

import swift.clocks.Timestamp;

public interface PubSub<T> {

    interface Notifyable<T> {

        Object src();

        T key();

        Set<T> keys();

        Timestamp timestamp();

        void notifyTo(PubSub<T> pubsub);
    }

    interface Subscriber<T> {

        String id();

        void onNotification(final Notifyable<T> info);
    }

    interface Publisher<T, P extends Notifyable<T>> {

        void publish(P info);
    }

    String id();

    boolean subscribe(T key, Subscriber<T> handler);

    boolean subscribe(Set<T> keys, Subscriber<T> handler);

    boolean unsubscribe(T key, Subscriber<T> handler);

    boolean unsubscribe(Set<T> keys, Subscriber<T> handler);

    Set<Subscriber<T>> subscribers(T key, boolean clone);

    Set<Subscriber<T>> subscribers(Set<T> keys);

    boolean isSubscribed(T key);

    boolean isSubscribed(T key, Subscriber<T> handler);
}
