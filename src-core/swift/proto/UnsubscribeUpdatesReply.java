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

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class UnsubscribeUpdatesReply implements RpcMessage, MetadataSamplable {

    protected long id;

    UnsubscribeUpdatesReply() {
    }

    public UnsubscribeUpdatesReply(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        final Kryo kryo = collector.getKryo();
        final Output buffer = collector.getKryoBuffer();

        // TODO: capture from the wire, rather than recompute here
        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();
        collector.recordStats(this, totalSize, 0, 0);
    }
}
