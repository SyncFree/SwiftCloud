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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.CausalityClock;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Scout request to update its subscriptions.
 * 
 * @author smduarte
 */
public class PubSubHandshakeReply implements RpcMessage, MetadataSamplable {

    /**
     * For Kryo, do NOT use.
     */
    public PubSubHandshakeReply() {
    }

    // CausalityClock clock;

    public PubSubHandshakeReply(CausalityClock clock) {
        // this.clock = clock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isMessageReportEnabled()) {
            return;
        }
        Kryo kryo = collector.getFreshKryo();
        Output buffer = collector.getFreshKryoBuffer();
        kryo.writeObject(buffer, this);
        final int size = buffer.position();
        collector.recordMessageStats(this, size, 0, 0, 0, 0, 0, 0);
        // clock != null ? clock.getSize() : 0,
        // clock != null ? clock.getExceptionsNumber() : 0);
    }
}
