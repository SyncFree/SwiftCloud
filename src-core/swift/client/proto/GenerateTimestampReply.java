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

import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Timestamp given by the server to the client.
 * <p>
 * DEPRECATED, client does not request a timestamp directly anymore!
 * 
 * @author mzawirski
 */
public class GenerateTimestampReply implements RpcMessage {
    protected Timestamp timestamp;
    protected long validityMillis;

    // Fake constructor for Kryo serialization. Do NOT use.
    GenerateTimestampReply() {
    }

    public GenerateTimestampReply(final Timestamp timestamp, final long validityMillis) {
        this.timestamp = timestamp;
        this.validityMillis = validityMillis;
    }

    /**
     * @return timestamp that client can use, subject to renewal using keepalive
     *         message
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return until what time the timestamp stays valid unless extended using
     *         keepalive; specified in milliseconds since the UNIX epoch; after
     *         that time client committing updates using this timestamp might be
     *         rejected to commit
     */
    public long getValidityMillis() {
        return validityMillis;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((GenerateTimestampReplyHandler) handler).onReceive(conn, this);
    }
}
