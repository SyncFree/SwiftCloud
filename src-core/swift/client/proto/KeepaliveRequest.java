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
import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to keepalive certain state at the server/storage-side.
 * Currently used to renew validity of a timestamp and/or keep version of
 * objects alive (not pruned).
 * 
 * @author mzawirski
 */
public class KeepaliveRequest extends ClientRequest {
    protected Timestamp timestamp;
    protected CausalityClock version;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    KeepaliveRequest() {
    }

    public KeepaliveRequest(String clientId, final Timestamp timestamp, final CausalityClock version) {
        super(clientId);
        this.timestamp = timestamp;
        this.version = version;
    }

    /**
     * @return the timestamp previously received from the server, subject to
     *         validity renewal by server; null if client does not request to
     *         renew the validity of any timestamp
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return the oldest snapshot in use by the client; the server should keep
     *         the requested (and later) versions of all objects; null if client
     *         does not request to keep any version of objects
     */
    public CausalityClock getVersion() {
        return version;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((BaseServer) handler).onReceive(conn, this);
    }
}
