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

import java.util.LinkedList;
import java.util.List;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Client batch request to commit a a sequence of transactions. Transactions
 * should be committed sequentially and a depedency clock of <em>i+1</em>-th
 * should be updated to include system timestamp of <em>i</em>-th transaction.
 * 
 * @author mzawirski
 */
public class BatchCommitUpdatesRequest extends ClientRequest {
    protected LinkedList<CommitUpdatesRequest> commitRequests;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    BatchCommitUpdatesRequest() {
    }

    /**
     * Creates a batch commit request made of provided list of requests.
     * 
     * @param clientId
     *            client id
     * @param commitRequests
     *            sequence of commit requests from the client
     */
    public BatchCommitUpdatesRequest(String clientId, List<CommitUpdatesRequest> commitRequests) {
        super(clientId);
        this.commitRequests = new LinkedList<CommitUpdatesRequest>(commitRequests);
    }

    /**
     * @return sequence of commit updates requests, one possibly dependent on
     *         another
     */
    // TODO: Weaken the dependencies?
    public LinkedList<CommitUpdatesRequest> getCommitRequests() {
        return commitRequests;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

}
