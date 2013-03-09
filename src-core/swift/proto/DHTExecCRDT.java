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

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.dc.DHTDataNode;
import sys.dht.api.DHT;

/**
 * Object for executing operations in a crdt
 * 
 * @author preguica
 */
public class DHTExecCRDT implements DHT.Message {

    CRDTObjectUpdatesGroup<?> grp;
    CausalityClock curDCVersion;
    CausalityClock snapshotVersion;
    CausalityClock trxVersion;
    Timestamp txTs;
    Timestamp cltTs;
    Timestamp prvCltTs;

    /**
     * Needed for Kryo serialization
     */
    DHTExecCRDT() {
    }

    public DHTExecCRDT(CRDTObjectUpdatesGroup<?> grp, CausalityClock snapshotVersion, CausalityClock trxVersion,
            Timestamp txTs, Timestamp cltTs, Timestamp prvCltTs, CausalityClock curDCVersion) {
        this.grp = grp;
        this.snapshotVersion = snapshotVersion;
        this.trxVersion = trxVersion;
        this.txTs = txTs;
        this.cltTs = cltTs;
        this.prvCltTs = prvCltTs;
        this.curDCVersion = curDCVersion;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((DHTDataNode.RequestHandler) handler).onReceive(conn, key, this);
    }

    public CRDTObjectUpdatesGroup<?> getGrp() {
        return grp;
    }

    public CausalityClock getSnapshotVersion() {
        return snapshotVersion;
    }

    public CausalityClock getTrxVersion() {
        return trxVersion;
    }

    public Timestamp getTxTs() {
        return txTs;
    }

    public Timestamp getCltTs() {
        return cltTs;
    }

    public Timestamp getPrvCltTs() {
        return prvCltTs;
    }

    public CausalityClock getCurDCVersion() {
        return curDCVersion;
    }

}
