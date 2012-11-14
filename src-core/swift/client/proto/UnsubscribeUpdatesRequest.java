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
package swift.client.proto;

import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to unsubscribe update notifications for an object.
 * 
 * @author mzawirski
 */
public class UnsubscribeUpdatesRequest extends ClientRequest {
    protected CRDTIdentifier uid;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    UnsubscribeUpdatesRequest() {
    }

    public UnsubscribeUpdatesRequest(String clientId, CRDTIdentifier uid) {
        super(clientId);
        this.uid = uid;
    }

    /**
     * @return object id to unsubscribe
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
