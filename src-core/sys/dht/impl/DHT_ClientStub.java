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

public class DHT_ClientStub implements DHT {
    private static Logger Log = Logger.getLogger(DHT_ClientStub.class.getName());

    RpcEndpoint myEndpoint;
    final AtomicReference<Endpoint> dhtEndpoint = new AtomicReference<Endpoint>();

    @Override
    public Endpoint localEndpoint() {
        return myEndpoint.localEndpoint();
    }

    public DHT_ClientStub(final Endpoint dhtEndpoint) {
        this.dhtEndpoint.set(dhtEndpoint);
        this.myEndpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toService(RpcServices.DHT.ordinal());
    }

    DHT_ClientStub(final RpcEndpoint myEndpoint, final Endpoint dhtEndpoint) {
        this.myEndpoint = myEndpoint;
        this.dhtEndpoint.set(dhtEndpoint);
    }

    @Override
    public void send(final Key key, final DHT.Message msg) {
        this.send(dhtEndpoint.get(), new DHT_Request(key, msg), DHT_CLIENT_TIMEOUT);
    }

    @Override
    public void send(final Key key, final DHT.Message msg, final DHT.ReplyHandler handler) {
        DHT_RequestReply reply = this.send(dhtEndpoint.get(), new DHT_Request(key, msg, true), DHT_CLIENT_TIMEOUT);
        if (reply != null)
            if (reply.payload != null)
                reply.payload.deliverTo(new DHT_Handle(null, false), handler);
            else
                handler.onFailure();
    }

    @Override
    public void send(final Key key, final DHT.Message msg, int timeout) {
        this.send(dhtEndpoint.get(), new DHT_Request(key, msg), timeout);
    }

    @Override
    public void send(final Key key, final DHT.Message msg, final DHT.ReplyHandler handler, int timeout) {
        DHT_RequestReply reply = this.send(dhtEndpoint.get(), new DHT_Request(key, msg, true), timeout);
        if (reply != null)
            if (reply.payload != null)
                reply.payload.deliverTo(new DHT_Handle(null, false), handler);
            else
                handler.onFailure();
    }

    @Override
    public Endpoint resolveKey(final Key key, int timeout) {
        final AtomicReference<Endpoint> ref = new AtomicReference<Endpoint>();
        myEndpoint.send(dhtEndpoint.get(), new DHT_ResolveKey(key), new DHT_StubHandler() {
            public void onReceive(final RpcHandle conn, final DHT_ResolveKeyReply reply) {
                if (key.equals(reply.key))
                    ref.set(reply.endpoint);
            }
        }, timeout);
        return ref.get();
    }

    public DHT_RequestReply send(final Endpoint dst, final DHT_Request req) {
        return send(dst, req, DHT_CLIENT_TIMEOUT);
    }

    public DHT_RequestReply send(final Endpoint dst, final DHT_Request req, int timeout) {
        final AtomicReference<DHT_RequestReply> ref = new AtomicReference<DHT_RequestReply>(null);
        for (int i = 0; ref.get() == null && i < DHT_CLIENT_RETRIES; i++) {
            myEndpoint.send(dhtEndpoint.get(), req, new DHT_StubHandler() {

                public void onFailure(RpcHandle handle) {
                }

                public void onReceive(final RpcHandle handle, final DHT_RequestReply reply) {
                    ref.set(reply);
                }

                public void onReceive(final RpcHandle handle, final DHT_ResolveKeyReply reply) {
                    dhtEndpoint.set(reply.endpoint);
                    Log.finest(String.format("Got redirection for key: %s to %s", req.key, reply.endpoint));
                }

            }, timeout);
        }
        return ref.get();
    }

}
