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

import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.dc.*;
import sys.dht.api.DHT;

/**
 * 
 * @author preguica
 * 
 */
public class DHTExecCRDTReply implements DHT.Reply {
    ExecCRDTResult result;

    public DHTExecCRDTReply(ExecCRDTResult result) {
        this.result = result;
    }

    /**
     * Needed for Kryo serialization
     */
    DHTExecCRDTReply() {
    }

    @Override
    public void deliverTo(DHT.Handle conn, DHT.ReplyHandler handler) {
        if (conn.expectingReply())
            ((DHTExecCRDTReplyHandler) handler).onReceive(conn, this);
        else
            ((DHTExecCRDTReplyHandler) handler).onReceive(this);
    }

    public ExecCRDTResult getResult() {
        return result;
    }

}
