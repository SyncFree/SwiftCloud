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

import sys.dht.api.DHT;
import sys.dht.api.DHT.Reply;
import sys.dht.api.DHT.ReplyHandler;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;

public class DHT_Handle implements DHT.Handle {

    RpcHandle handle;
    boolean expectingReply;

    DHT_Handle(RpcHandle handle, boolean expectingReply) {
        this.handle = handle;
        this.expectingReply = expectingReply;
    }

    @Override
    public boolean expectingReply() {
        return expectingReply;
    }

    @Override
    public boolean reply(Reply msg) {
        return handle.reply(new DHT_RequestReply(msg)).succeeded();
    }

    @Override
    public boolean reply(Reply msg, ReplyHandler handler) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Endpoint remoteEndpoint() {
        return handle.remoteEndpoint();
    }
}
