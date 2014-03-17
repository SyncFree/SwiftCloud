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
import swift.crdt.core.CRDTIdentifier;
import swift.dc.DHTDataNode;
import sys.dht.api.DHT;

/**
 * Object for getting a crdt
 * 
 * @author preguica
 */
public class DHTGetCRDT implements DHT.Message {

    String clientId;
    CRDTIdentifier id;
    CausalityClock version;
    boolean subscribeUpdates;

    /**
     * Needed for Kryo serialization
     */
    DHTGetCRDT() {
    }

    public DHTGetCRDT(CRDTIdentifier id, CausalityClock version, String clientId, boolean subscribeUpdates) {
        super();
        this.id = id;
        this.version = version;
        this.clientId = clientId;
        this.subscribeUpdates = subscribeUpdates;
    }

    public CRDTIdentifier getId() {
        return id;
    }

    public CausalityClock getVersion() {
        return version;
    }

    public boolean subscribesUpdates() {
        return subscribeUpdates;
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.Key key, DHT.MessageHandler handler) {
        ((DHTDataNode.RequestHandler) handler).onReceive(conn, key, this);
    }

    public String getCltId() {
        return clientId;
    }
}
