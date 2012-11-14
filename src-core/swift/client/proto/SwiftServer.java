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
package swift.client.proto;

import sys.net.api.rpc.RpcHandle;

/**
 * Server interface for client-server interaction.
 * <p>
 * This interface defines interaction between clients and a single server, i.e.
 * sessions with affinity. Server can demultiplex different clients using client
 * id available through {@link ClientRequest#getClientId()}. For documentation
 * of particular requests, see message definitions.
 * 
 * @author mzawirski
 */
public interface SwiftServer extends BaseServer {
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link FetchObjectVersionReplyHandler} and expects
     *            {@link FetchObjectVersionReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, FetchObjectVersionRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link FetchObjectVersionReplyHandler} and expects
     *            {@link FetchObjectVersionReply}
     * @param request
     *            request to serve
     */
    // TODO: FetchObjectVersionReply is temporary. Eventually, we shall replace
    // it with deltas or list of operations.
    void onReceive(RpcHandle conn, FetchObjectDeltaRequest request);

    /**
     * @param conn
     *            connection that does not expect any message
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, UnsubscribeUpdatesRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link RecentUpdatesReplyHandler} and expects
     *            {@link RecentUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, RecentUpdatesRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link FastRecentUpdatesReplyHandler} and expects
     *            {@link FastRecentUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, FastRecentUpdatesRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link CommitUpdatesReplyHandler} and expects
     *            {@link CommitUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, CommitUpdatesRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link BatchCommitUpdatesReplyHandler} and expects
     *            {@link BatchCommitUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, BatchCommitUpdatesRequest request);
}
