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

import java.util.ArrayList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client request to commit set of updates to the store.
 * <p>
 * All updates use the same client timestamp and will receive a system
 * timestamp(s) during commit. Updates are organized into atomic groups of
 * updates per each object.
 * 
 * @author mzawirski
 */
public class CommitUpdatesRequest extends ClientRequest {

    protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    protected CausalityClock dependencyClock;

    transient Timestamp timestamp;
    protected Timestamp cltTimestamp;

    transient boolean disasterSafe;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    CommitUpdatesRequest() {
    }

    public CommitUpdatesRequest(String clientId, boolean disasterSafeSession, final Timestamp cltTimestamp,
            final CausalityClock dependencyClock, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
        super(clientId, disasterSafeSession);
        this.cltTimestamp = cltTimestamp;
        this.dependencyClock = dependencyClock;
        this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(objectUpdateGroups);
    }

    public CausalityClock getDependencyClock() {
        return dependencyClock;
    }

    /**
     * Set the waitTilStable
     */
    public void setDisasterSafe() {
        this.disasterSafe = true;
    }

    /**
     * Flag to tell if this commit requires stability
     */
    public boolean disasterSafe() {
        return disasterSafe;
    }

    /**
     * Record the timestamp obtained for this commit from the sequencer
     * 
     * @param timestamp
     *            from the sequencer
     */
    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return timestamp obtained from the sequencer...
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return valid base timestamp for all updates in the request, previously
     *         obtained using {@link GenerateTimestampRequest}; all individual
     *         updates use TripleTimestamps with this base Timestamp
     */
    public Timestamp getCltTimestamp() {
        return cltTimestamp;
    }

    /**
     * @return list of groups of object operations; there is at most one group
     *         per object; note that all groups share the same base client
     *         timestamp ( {@link #getClientTimestamp()}), timestamp mappings
     *         and dependency clock.
     * 
     */
    public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
        return objectUpdateGroups;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    public void addTimestampsToDeps(List<Timestamp> tsLst) {
        if (tsLst != null) {
            for (Timestamp t : tsLst) {
                this.dependencyClock.record(t);
            }
        }
    }

    @Override
    public String toString() {
        return "CommitUpdatesRequest [objectUpdateGroups=" + objectUpdateGroups + ", dependencyClock="
                + dependencyClock + ", cltTimestamp=" + cltTimestamp + "]";
    }
}
