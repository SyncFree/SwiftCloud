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

import java.util.List;

import swift.dc.DCSequencerServer;
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
public class MultipleSeqCommitUpdatesRequest implements RpcMessage {

    public List<SeqCommitUpdatesRequest> commitRecords;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    MultipleSeqCommitUpdatesRequest() {
    }

    public MultipleSeqCommitUpdatesRequest(List<SeqCommitUpdatesRequest> records) {
        this.commitRecords = records;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((DCSequencerServer) handler).onReceive(conn, this);
    }
}
