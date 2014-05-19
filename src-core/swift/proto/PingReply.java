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

import swift.clocks.Timestamp;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Used to check RTT and clock skew.
 * 
 * @author nmp
 */
public class PingReply implements RpcMessage {
    protected long timeAtSender;
    protected long timeAtReceiver;

    // Fake constructor for Kryo serialization. Do NOT use.
    PingReply() {
    }

    public PingReply(long timeAtSender, long currentTime) {
        this.timeAtSender = timeAtSender;
        this.timeAtReceiver = currentTime;
    }


    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) { // If used as a
                                                                // reply to
                                                                // request()
                                                                // call, no
                                                                // extra
                                                                // processing
                                                                // required.
        // ((GenerateDCTimestampReplyHandler) handler).onReceive(conn, this);
    }

    public long getTimeAtSender() {
        return timeAtSender;
    }

    public long getTimeAtReceiver() {
        return timeAtReceiver;
    }
}
