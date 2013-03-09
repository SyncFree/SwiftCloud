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

import swift.pubsub.CommitNotification;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Scout request to update its subscriptions.
 * 
 * @author smduarte
 */
public class UpdatesNotification implements RpcMessage {

    protected int seqN;
    protected List<CommitNotification> records;

    /**
     * For Kryo, do NOT use.
     */
    UpdatesNotification() {
    }

    public UpdatesNotification(int seqN, List<CommitNotification> records) {
        this.seqN = seqN;
        this.records = records;
    }

    public int seqN() {
        return seqN;
    }

    public List<CommitNotification> getRecords() {
        return records;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }
}
