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

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to client committs message.
 * 
 * @author preguica
 */
public class CommitTSReply implements RpcMessage {
    public enum CommitTSStatus {
        /**
         * The reply contains requested version.
         */
        OK,
        /**
         * The requested object is not in the store.
         */
        FAILED
    }

    protected CommitTSStatus status;
    protected CausalityClock currVersion;
    protected CausalityClock stableVersion;

    public CommitTSReply() {
    }

    public CommitTSReply(CommitTSStatus status, CausalityClock currVersion, CausalityClock stableVersion) {
        super();
        this.status = status;
        this.currVersion = currVersion;
        this.stableVersion = stableVersion;
    }

    /**
     * @return status code of the reply
     */
    public CommitTSStatus getStatus() {
        return status;
    }

    /**
     * @return the current version in the server
     */
    public CausalityClock getCurrVersion() {
        return currVersion;
    }

    /**
     * @return the current version in the server
     */
    public CausalityClock getStableVersion() {
        return stableVersion;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((CommitTSReplyHandler) handler).onReceive(conn, this);
    }
}
