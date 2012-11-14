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
 * Client request to generate a timestamp for a transaction.
 * <p>
 * DEPRECATED, client does not request a timestamp directly anymore!
 * 
 * @author mzawirski
 */
public class GenerateTimestampRequest extends ClientRequest {
    protected CausalityClock dominatedClock;
    protected Timestamp previousTimestamp;

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateTimestampRequest() {
    }

    public GenerateTimestampRequest(String clientId, CausalityClock dominatedClock, Timestamp previousTimestamp) {
        super(clientId);
        this.dominatedClock = dominatedClock;
        this.previousTimestamp = previousTimestamp;
    }

    /**
     * @return the clock that the requested timestamp should dominate (to
     *         enforce invariant that later timestamp is never dominated by
     *         earlier)
     */
    public CausalityClock getDominatedClock() {
        return dominatedClock;
    }

    /**
     * @return optional previous timestamp acquired from some server, invalid /
     *         rejected; can be null
     */
    public Timestamp getPreviousTimestamp() {
        return previousTimestamp;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((BaseServer) handler).onReceive(conn, this);
    }
}
