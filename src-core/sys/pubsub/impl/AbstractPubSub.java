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

import java.util.Set;

import sys.net.api.Endpoint;
import sys.pubsub.PubSub;

public class AbstractPubSub<K, P> implements PubSub<K, P>, PubSub.Handler<K, P> {

    @Override
    public void publish(K key, P info) {
        Thread.dumpStack();
    }

    @Override
    public void publish(Set<K> key, P info) {
        Thread.dumpStack();
    }

    @Override
    public void subscribe(K key, sys.pubsub.PubSub.Handler<K, P> handler) {
        Thread.dumpStack();
    }

    @Override
    public void unsubscribe(K key, sys.pubsub.PubSub.Handler<K, P> handler) {
        Thread.dumpStack();
    }

    @Override
    public void addRemoteSubscriber(K key, Endpoint subscriber) {
        Thread.dumpStack();
    }

    @Override
    public void notify(K key, P info) {
        Thread.dumpStack();
    }

    @Override
    public void notify(Set<K> key, P info) {
        Thread.dumpStack();
    }

}
