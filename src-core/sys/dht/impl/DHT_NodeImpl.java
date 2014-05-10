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

import static sys.Sys.Sys;
import static sys.dht.catadupa.Config.Config;

import java.util.logging.Logger;

import sys.RpcServices;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Handle;
import sys.dht.api.DHT.ReplyHandler;
import sys.dht.catadupa.CatadupaNode;
import sys.dht.catadupa.Node;
import sys.dht.discovery.Discovery;
import sys.dht.impl.msgs.DHT_Request;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.dht.impl.msgs.DHT_ResolveKey;
import sys.dht.impl.msgs.DHT_ResolveKeyReply;
import sys.dht.impl.msgs.DHT_StubHandler;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Threading;

public class DHT_NodeImpl extends CatadupaNode {
    private static Logger Log = Logger.getLogger(DHT_NodeImpl.class.getName());

    public long ordinalKey = 0;

    protected static DHT_ClientStub clientStub;
    protected static _DHT_ServerStub serverStub;

    protected DHT_NodeImpl() {
    }

    @Override
    public void init() {
        super.init();

        while (!super.isReady())
            Threading.sleep(50);

        serverStub = new _DHT_ServerStub(new DHT.MessageHandler() {

            @Override
            public void onFailure() {
                Thread.dumpStack();
            }

            @Override
            public void onReceive(Handle conn, DHT.Key key, DHT.Message m) {
                Log.finest(String.format("Un-handled DHT message [<%s,%s>]", key, m.getClass()));
            }
        });

        String name = DHT_Node.DHT_ENDPOINT + Sys.getDatacenter();
        Discovery.register(name, serverStub.getEndpoint().localEndpoint());

        clientStub = new DHT_ClientStub(serverStub.getEndpoint(), serverStub.getEndpoint().localEndpoint());
    }

    @Override
    public void onNodeAdded(Node n) {
        Log.finest("Catadupa added:" + n + "; key:" + n.key);
    }

    @Override
    public void onNodeRemoved(Node n) {
        Log.finest("Catadupa removed:" + n + "; key:" + n.key);
    }

    protected Node resolveNextHop(final DHT.Key key) {
        long key2key = key.longHashValue() % (1L << Config.NODE_KEY_LENGTH);
        Log.finest(String.format("Hashing %s (%s) @ %s DB:%s", key, key2key, self.key, odb.nodeKeys()));

        for (Node i : super.odb.nodes(key2key))
            if (i.isOnline())
                return i;

        return self;
    }

    protected class _DHT_ServerStub extends DHT_StubHandler {

        DHT.MessageHandler myHandler;
        final RpcEndpoint myEndpoint;

        _DHT_ServerStub(DHT.MessageHandler myHandler) {
            this.myHandler = myHandler;
            myEndpoint = rpcFactory.toService(RpcServices.DHT.ordinal(), this);
        }

        RpcEndpoint getEndpoint() {
            return myEndpoint;
        }

        public void setHandler(DHT.MessageHandler handler) {
            myHandler = handler;
        }

        public void onReceive(final RpcHandle conn, final DHT_ResolveKey req) {
            Node nextHop = resolveNextHop(req.key);
            if (nextHop != null)
                conn.reply(new DHT_ResolveKeyReply(req.key, nextHop.endpoint));
        }

        @Override
        public void onReceive(RpcHandle handle, DHT_Request req) {
            Thread.dumpStack();

            Node nextHop = resolveNextHop(req.key);
            System.err.println(self.key + "   Got request for: " + req.key + " next hop: " + nextHop + "/ "
                    + req.redirected);
            if (nextHop != null && nextHop.key != self.key && !req.redirected) {
                // handle.reply( new DHT_ResolveKeyReply(req.key,
                // nextHop.endpoint) ) ;
                req.redirected = true;
                DHT_RequestReply reply = clientStub.send(nextHop.endpoint, req);
                if (reply != null)
                    handle.reply(reply);
            } else {
                req.payload.deliverTo(new DHT_Handle(handle, req.expectingReply), req.key, myHandler);
                if (!req.expectingReply)
                    handle.reply(new DHT_RequestReply(null));
            }
        }
    }

}

class FOO implements DHT.Reply {

    FOO() {
    }

    @Override
    public void deliverTo(Handle conn, ReplyHandler handler) {
        Thread.dumpStack();
    }

}