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

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Message with committted transaction.
 * <p>
 * All updates use the same timestamp. Updates are organized into atomic groups
 * of updates per each object.
 * 
 * @author preguica
 */
public class SeqCommitUpdatesRequest extends CommitUpdatesRequest {

    Timestamp timestamp;
    Timestamp prvCltTimestamp;
    CausalityClock dcReceived;
    CausalityClock dcNotUsed;

    public transient long lastSent;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    SeqCommitUpdatesRequest() {
    }

    public SeqCommitUpdatesRequest(String dcName, final Timestamp timestamp, final Timestamp cltTimestamp,
            final Timestamp prvCltTimestamp, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups,
            CausalityClock dcReceived, CausalityClock dcNotUsed) {
        super(dcName, false, cltTimestamp, null, objectUpdateGroups);
        this.timestamp = timestamp;
        this.prvCltTimestamp = prvCltTimestamp;
        this.dcReceived = dcReceived;
        this.dcNotUsed = dcNotUsed;
    }

    /**
     * @return timestamp obtained from the sequencer...
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    public CausalityClock getDcReceived() {
        return dcReceived;
    }

    public CausalityClock getDcNotUsed() {
        return dcNotUsed;
    }

    public String getDcName() {
        return super.getClientId();
    }

    public Timestamp getPrvCltTimestamp() {
        return prvCltTimestamp;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }
}
