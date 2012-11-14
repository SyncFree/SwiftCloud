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
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Server reply to client keepalive message.
 * 
 * @author mzawirski
 */
public class KeepaliveReply implements RpcMessage {
    protected boolean timestampRenewed;
    protected boolean versionAvailable;
    protected long validityMillis;

    KeepaliveReply() {
    }

    public KeepaliveReply(boolean timestampRenewed, boolean versionAvailable, long validityMillis) {
        this.timestampRenewed = timestampRenewed;
        this.versionAvailable = versionAvailable;
        this.validityMillis = validityMillis;
    }

    /**
     * @return true if timestamp validity renewal was requested and successfully
     *         renewed
     */
    public boolean isTimestampRenewed() {
        return timestampRenewed;
    }

    /**
     * @return true if version keepalive was requested and all objects are still
     *         available in this version
     */
    public boolean isVersionAvailable() {
        return versionAvailable;
    }

    /**
     * @return until what time the timestamp & objects stay available unless
     *         extended using keepalive; specified in milliseconds since the
     *         UNIX epoch
     */
    public long getValidityMillis() {
        return validityMillis;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((KeepaliveReplyHandler) handler).onReceive(conn, this);
    }
}
