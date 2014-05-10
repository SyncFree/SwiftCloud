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
package swift.proto;

import swift.crdt.core.ManagedCRDT;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * 
 * @author preguica
 * 
 */
public class DHTGetCRDTReply implements RpcMessage {

    ManagedCRDT object;

    /**
     * Needed for Kryo serialization
     */
    DHTGetCRDTReply() {
    }

    public DHTGetCRDTReply(ManagedCRDT object) {
        this.object = object;
    }

    // @Override
    // public void deliverTo( RpcHandler conn, DHT.ReplyHandler handler) {
    // System.err.println("Delivering to:" + handler.getClass());
    // if (conn.expectingReply())
    // ((SwiftProtocolHandler) handler).onReceive(conn, this);
    // else
    // ((SwiftProtocolHandler) handler).onReceive(this);
    // }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(this);
    }

    public ManagedCRDT getObject() {
        return object;
    }

}
