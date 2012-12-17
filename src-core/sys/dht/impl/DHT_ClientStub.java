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
package sys.dht.impl;

import static sys.net.api.Networking.Networking;
import static sys.net.impl.NetworkingConstants.DHT_CLIENT_RETRIES;
import static sys.net.impl.NetworkingConstants.DHT_CLIENT_TIMEOUT;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import sys.RpcServices;
import sys.dht.api.DHT;
import sys.dht.impl.msgs.DHT_Request;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.dht.impl.msgs.DHT_ResolveKey;
import sys.dht.impl.msgs.DHT_ResolveKeyReply;
import sys.dht.impl.msgs.DHT_StubHandler;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

public class DHT_ClientStub implements DHT {
    private static Logger Log = Logger.getLogger(DHT_ClientStub.class.getName());

    // private static final int RETRIES = 3;
    // private static final int TIMEOUT = 100;

    Endpoint dhtEndpoint;
    RpcEndpoint myEndpoint;

    @Override
    public Endpoint localEndpoint() {
        return myEndpoint.localEndpoint();
    }

    public DHT_ClientStub(final Endpoint dhtEndpoint) {
        this.dhtEndpoint = dhtEndpoint;
        myEndpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toService(RpcServices.DHT.ordinal());
    }

    DHT_ClientStub(final RpcEndpoint myEndpoint, final Endpoint dhtEndpoint) {
        this.myEndpoint = myEndpoint;
        this.dhtEndpoint = dhtEndpoint;
    }

    @Override
    public void send(final Key key, final DHT.Message msg) {
        this.send(dhtEndpoint, new DHT_Request(key, msg));
    }

    @Override
    public void send(final Key key, final DHT.Message msg, final DHT.ReplyHandler handler) {
        DHT_RequestReply reply = this.send(dhtEndpoint, new DHT_Request(key, msg, true));
        if (reply != null)
            if (reply.payload != null)
                reply.payload.deliverTo(new DHT_Handle(null, false), handler);
            else
                handler.onFailure();
    }

    @Override
    public Endpoint resolveKey(final Key key, int timeout) {
        final AtomicReference<Endpoint> ref = new AtomicReference<Endpoint>();
        myEndpoint.send(dhtEndpoint, new DHT_ResolveKey(key), new DHT_StubHandler() {
            public void onReceive(final RpcHandle conn, final DHT_ResolveKeyReply reply) {
                if (key.equals(reply.key))
                    ref.set(reply.endpoint);
            }
        }, timeout);
        return ref.get();
    }

    public DHT_RequestReply send(final Endpoint dst, final DHT_Request req) {
        final AtomicInteger delay = new AtomicInteger(50);
        final AtomicReference<Endpoint> dhtNode = new AtomicReference<Endpoint>(dst);
        final AtomicReference<DHT_RequestReply> ref = new AtomicReference<DHT_RequestReply>(null);
        for (int i = 0; i < DHT_CLIENT_RETRIES; i++) {
            myEndpoint.send(dhtNode.get(), req, new DHT_StubHandler() {

                public void onFailure(RpcHandle handle) {
                }

                public void onReceive(final RpcHandle handle, final DHT_RequestReply reply) {
                    ref.set(reply);
                }

                public void onReceive(final RpcHandle handle, final DHT_ResolveKeyReply reply) {
                    delay.set(0);
                    dhtNode.set(reply.endpoint);
                    Log.finest(String.format("Got redirection for key: %s to %s", req.key, reply.endpoint));
                }

            }, DHT_CLIENT_TIMEOUT);
            if (ref.get() != null)
                break;
            else
                Threading.sleep(delay.getAndAdd((i + 1) * 100));
        }
        return ref.get();
    }

}
