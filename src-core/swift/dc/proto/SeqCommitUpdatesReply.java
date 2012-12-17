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
package swift.dc.proto;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server confirmation of committed updates.
 * 
 * @author preguica
 * @see SeqCommitUpdatesRequest
 */
public class SeqCommitUpdatesReply implements RpcMessage {
    protected String dcName;
    protected CausalityClock dcClock; // applied operations at given data center
    protected CausalityClock dcStableClock; // stable operations at given data
                                            // center
    protected CausalityClock dcKnownClock; // known operations at given data
                                           // center

    /**
     * Fake constructor for Kryo serialization. Do NOT use. REMARK: smd, however
     * it is being used by the sequencer????
     */
    public SeqCommitUpdatesReply() {
    }

    public SeqCommitUpdatesReply(String dcName, CausalityClock dcClock, CausalityClock stableClock,
            CausalityClock knownClock) {
        this.dcName = dcName;
        this.dcClock = dcClock;
        this.dcStableClock = stableClock;
        this.dcKnownClock = knownClock;
    }

    /**
     * @return DC clock
     */
    public CausalityClock getDCClock() {
        return dcClock;
    }

    /**
     * @return stable clock
     */
    public CausalityClock getDCStableClock() {
        return dcStableClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SeqCommitUpdatesReplyHandler) handler).onReceive(conn, this);
    }

    public CausalityClock getDcKnownClock() {
        return dcKnownClock;
    }

    public String getDcName() {
        return dcName;
    }
}
