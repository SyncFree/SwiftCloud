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
import sys.net.api.rpc.RpcMessage;

/**
 * Informs the Sequencer Server that the given timestamp should be
 * committed/rollbacked.
 * 
 * @author preguica
 */
public class CommitTSRequest implements RpcMessage {
    protected Timestamp timestamp;
    protected Timestamp cltTimestamp;
    protected Timestamp prvCltTimestamp;
    protected CausalityClock version; // observed version
    protected boolean commit; // true if transaction was committed
    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;

    protected boolean disasterSafe;

    public String clientId;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitTSRequest() {
    }

    public CommitTSRequest(Timestamp timestamp, Timestamp cltTimestamp, Timestamp prvCltTimestamp,
            CausalityClock version, boolean commit, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.prvCltTimestamp = prvCltTimestamp;
        this.version = version;
        this.commit = commit;
        this.objectUpdateGroups = objectUpdateGroups;
        this.disasterSafe = true;
    }

    public CommitTSRequest(Timestamp timestamp, Timestamp cltTimestamp, Timestamp prvCltTimestamp,
            CausalityClock version, boolean commit, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups,
            boolean disasterSafe, String clientId) {
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.prvCltTimestamp = prvCltTimestamp;
        this.version = version;
        this.commit = commit;
        this.objectUpdateGroups = objectUpdateGroups;
        this.disasterSafe = disasterSafe;
        this.clientId = clientId;
    }

    public boolean disasterSafe() {
        return disasterSafe;
    }

    /**
     * @return the timestamp previously received from the server
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return the timestamp of the client
     */
    public Timestamp getCltTimestamp() {
        return cltTimestamp;
    }

    /**
     * @return the oldest snapshot in use by the client
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return true if this is called in a commit
     */
    public boolean getCommit() {
        return commit;
    }

    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

    public Timestamp getPrvCltTimestamp() {
        return prvCltTimestamp;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }
}
