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

import swift.dc.proto.SeqCommitUpdatesReply;
import swift.dc.proto.SeqCommitUpdatesReplyHandler;
import swift.dc.proto.SeqCommitUpdatesRequest;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

/**
 * Server interface for client-server interaction.
 * <p>
 * This interface defines the common functions of the surrogate and sequencer.
 */
public interface BaseServer extends RpcHandler {
    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link GenerateTimestampReplyHandler} and expects
     *            {@link GenerateTimestampReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, GenerateTimestampRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link KeepaliveReplyHandler} and expects
     *            {@link KeepaliveReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, KeepaliveRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link LatestKnownClockReplyHandler} and expects
     *            {@link LatestKnownClockReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, LatestKnownClockRequest request);

    /**
     * @param conn
     *            connection such that the remote end implements
     *            {@link SeqCommitUpdatesReplyHandler} and expects
     *            {@link SeqCommitUpdatesReply}
     * @param request
     *            request to serve
     */
    void onReceive(RpcHandle conn, SeqCommitUpdatesRequest request);
}
