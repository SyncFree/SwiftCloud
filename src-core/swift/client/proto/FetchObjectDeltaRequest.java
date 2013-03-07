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

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to get a delta between a known version and a specified version
 * of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectDeltaRequest extends FetchObjectVersionRequest {
    protected CausalityClock knownVersion;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    FetchObjectDeltaRequest() {
    }

    public FetchObjectDeltaRequest(String clientId, CRDTIdentifier id, CausalityClock knownVersion,
            CausalityClock version, boolean strictAvailableVersion, SubscriptionType subscribeUpdates) {
        super(clientId, id, version, strictAvailableVersion, subscribeUpdates);
        this.knownVersion = knownVersion;
    }

    /**
     * @return the latest version known by the client
     */
    public CausalityClock getKnownVersion() {
        return knownVersion;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
