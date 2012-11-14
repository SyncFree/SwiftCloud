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
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request for recent updates on previously subscribed objects (see
 * {@link FetchObjectVersionRequest#getSubscriptionType()}. Beside of
 * requesting recent updates, this request keep client subscriptions alive.
 * <p>
 * This call may block until new updates arrive, a timeout elapses on the server
 * (to inform of no updates) or server confirms a new subscription. Client
 * should repeatedly send this request in order to get new updates and keep her
 * subscriptions alive.
 * 
 * @author mzawirski
 */
public class RecentUpdatesRequest extends ClientRequest {
    protected CausalityClock lastClock;

    /**
     * No-args constructor for Kryo-serialization.
     */
    RecentUpdatesRequest() {
    }

    public RecentUpdatesRequest(final String clientId, final CausalityClock lastClock) {
        super(clientId);
        this.lastClock = lastClock;
    }

    /**
     * @return clock of previously received {@link RecentUpdatesReply}; null if
     *         client starts a new session without relying on previous messages
     */
    public CausalityClock getLastClock() {
        return lastClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
