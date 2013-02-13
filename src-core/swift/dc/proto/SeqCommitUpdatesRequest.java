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

import java.util.ArrayList;
import java.util.List;

import swift.client.proto.BaseServer;
import swift.client.proto.GenerateTimestampRequest;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Message with committted transaction.
 * <p>
 * All updates use the same timestamp. Updates are organized into atomic groups
 * of updates per each object.
 * 
 * @author preguica
 */
public class SeqCommitUpdatesRequest implements RpcMessage {
    protected String dcName;
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    protected Timestamp timestamp;
    protected Timestamp cltTimestamp;
    protected Timestamp prvCltTimestamp;
    CausalityClock dcReceived;
    CausalityClock dcNotUsed;
    public transient long lastSent;

    public long serial; // smd debug

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    SeqCommitUpdatesRequest() {
    }

    public SeqCommitUpdatesRequest(String dcName, final Timestamp timestamp, final Timestamp cltTimestamp,
            final Timestamp prvCltTimestamp, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups,
            CausalityClock dcReceived, CausalityClock dcNotUsed) {
        this.dcName = dcName;
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.prvCltTimestamp = prvCltTimestamp;
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups);
        this.dcReceived = dcReceived;
        this.dcNotUsed = dcNotUsed;
    }

    // smd debug
    public SeqCommitUpdatesRequest(String dcName, final Timestamp timestamp, final Timestamp cltTimestamp,
            final Timestamp prvCltTimestamp, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups,
            CausalityClock dcReceived, CausalityClock dcNotUsed, long serial) {
        this.dcName = dcName;
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.prvCltTimestamp = prvCltTimestamp;
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups);
        this.dcReceived = dcReceived;
        this.dcNotUsed = dcNotUsed;
        this.serial = serial;
    }

    /**
     * @return valid base timestamp for all updates in the request, previously
     *         obtained using {@link GenerateTimestampRequest}; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return client timestamp for this transaction; all individual updates use
     *         TripleTimestamps with this base Timestamp
     */
    public Timestamp getCltTimestamp() {
        return cltTimestamp;
    }

    /**
     * @return list of groups of object operations; there is at most one group
     *         per object and they all share the same base timestamp
     *         {@link #getBaseTimestamp()}
     */
    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((BaseServer) handler).onReceive(conn, this);
    }

    public CausalityClock getDcReceived() {
        return dcReceived;
    }

    public CausalityClock getDcNotUsed() {
        return dcNotUsed;
    }

    public String getDcName() {
        return dcName;
    }

    public Timestamp getPrvCltTimestamp() {
        return prvCltTimestamp;
    }
}
