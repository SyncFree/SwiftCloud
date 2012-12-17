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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import sys.pubsub.PubSub;

public class PubSubService<K, P> extends PubSub<K, P> {

    private int pubSubId;
    private RpcEndpoint svc;
    private RpcFactory factory;

    private Map<K, Set<Endpoint>> remoteSubscribers;
    private Map<K, Set<Handler<K, P>>> localSubscribers;

    public PubSubService(RpcEndpoint srv, int pubSubId) {
        this.factory = srv.getFactory();
        this.pubSubId = pubSubId;
        init();
    }

    public PubSubService(RpcFactory fac, int pubSubId) {
        this.factory = fac;
        this.pubSubId = pubSubId;
        init();
    }

    void init() {
        localSubscribers = new HashMap<K, Set<Handler<K, P>>>();
        remoteSubscribers = new HashMap<K, Set<Endpoint>>();

        svc = factory.toService(pubSubId, new PubSubRpcHandler<K, P>() {
            @Override
            public void onReceive(RpcHandle conn, PubSubNotification<K, P> m) {
                conn.reply(new PubSubAck(localSubscribers(m.key, false).size()));
                for (Handler<K, P> i : localSubscribers(m.key, true)) {
                    i.notify(m.key, m.info);
                }
            }
        });
    }

    // GC needs work.
    // Remove a remote subscriber upon first or a certain number of failures.
    // Use ack to reset suspicion?
    public void publish(final K key, final P info) {

        for (Endpoint i : remoteSubscribers(key, true)) {
            svc.send(i, new PubSubNotification<K, P>(key, info), new PubSubRpcHandler<K, P>() {
                public void onFailure(RpcHandle handle) {
                    removeRemoteSubscriber(key, handle.remoteEndpoint());
                }

                public void onReceive(final RpcHandle handle, final PubSubAck ack) {
                    if (ack.totalSubscribers() <= 0) {
                        removeRemoteSubscriber(key, handle.remoteEndpoint());
                    }
                }
            }, 0);
        }

        for (Handler<K, P> i : localSubscribers(key, true)) {
            i.notify(key, info);
        }
    }

    synchronized public void addRemoteSubscriber(K key, Endpoint subscriber) {
        // System.err.printf("Adding Remote :%s, %s\n", key, subscriber );
        remoteSubscribers(key, false).add(subscriber);
    }

    synchronized void removeRemoteSubscriber(K key, Endpoint subscriber) {
        remoteSubscribers(key, false).remove(subscriber);
    }

    synchronized public void subscribe(K key, Handler<K, P> handler) {
        // System.err.printf("Adding Local :%s, %s\n", key, handler );
        localSubscribers(key, false).add(handler);
    }

    synchronized public void unsubscribe(K key, Handler<K, P> handler) {
        localSubscribers(key, false).remove(handler);
    }

    synchronized private Set<Handler<K, P>> localSubscribers(K key, boolean clone) {
        Set<Handler<K, P>> res = localSubscribers.get(key);
        if (res == null)
            localSubscribers.put(key, res = new HashSet<Handler<K, P>>());
        return clone ? new HashSet<Handler<K, P>>(res) : res;
    }

    synchronized private Set<Endpoint> remoteSubscribers(K key, boolean clone) {
        Set<Endpoint> res = remoteSubscribers.get(key);
        if (res == null)
            remoteSubscribers.put(key, res = new HashSet<Endpoint>());
        return clone ? new HashSet<Endpoint>(res) : res;
    }
}
