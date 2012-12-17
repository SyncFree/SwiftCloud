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
package swift.dc;

import swift.dc.proto.DHTExecCRDT;
import swift.dc.proto.DHTExecCRDTReply;
import swift.dc.proto.DHTGetCRDT;
import swift.dc.proto.DHTGetCRDTReply;
import swift.dc.proto.DHTSendNotification;
import sys.dht.api.DHT;

/**
 * 
 * The KVS interface for DHT of DataNodes
 * 
 * @author preguica
 */
public interface DHTDataNode {

    /**
     * Denotes collection of requests/messages that the DHT DataNode
     * processes/expects
     * 
     * @author preguica
     * 
     */
    public abstract class RequestHandler extends DHT.AbstractMessageHandler {
        abstract public void onReceive(DHT.Handle con, DHT.Key key, DHTGetCRDT request);

        abstract public void onReceive(DHT.Handle con, DHT.Key key, DHTExecCRDT<?> request);
    }

    /**
     * Denotes collection of reply/messages that the DHT DataNode client
     * processes
     * 
     * @author preguica
     * 
     */
    public abstract class ReplyHandler extends DHT.AbstractReplyHandler {
        abstract public void onReceive(DHTGetCRDTReply reply);

        abstract public void onReceive(DHTExecCRDTReply reply);

        abstract public void onReceive(DHTSendNotification notification);
    }
}
