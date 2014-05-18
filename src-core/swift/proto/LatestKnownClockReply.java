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
 * Server reply to client's latest known clock request.
 * 
 * @author mzawirski
 */
public class LatestKnownClockReply implements RpcMessage, MetadataSamplable {
    private CausalityClock clock;
    private CausalityClock disasterDurableClock;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    LatestKnownClockReply() {
    }

    public LatestKnownClockReply(final CausalityClock clock, final CausalityClock disasterDurableClock) {
        this.clock = clock;
        this.disasterDurableClock = disasterDurableClock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate
     */
    public CausalityClock getClock() {
        return clock;
    }

    /**
     * @return latest known clock in the store, i.e. valid snapshot point
     *         candidate, durable in even in case of disaster affecting fragment
     *         of the store
     */
    public CausalityClock getDistasterDurableClock() {
        return disasterDurableClock;
    }

    @Override
    public void deliverTo(RpcHandle conn, RpcHandler handler) {
        // smd ((SwiftProtocolHandler) handler).onReceive(conn, this);
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        final Kryo kryo = collector.getFreshKryo();
        final Output buffer = collector.getFreshKryoBuffer();

        int maxExceptionsNum = 0;
        if (clock != null) {
            maxExceptionsNum = Math.max(clock.getExceptionsNumber(), maxExceptionsNum);
        }
        if (disasterDurableClock != null) {
            maxExceptionsNum = Math.max(disasterDurableClock.getExceptionsNumber(), maxExceptionsNum);
        }

        // TODO: capture from the wire, rather than recompute here
        kryo.writeObject(buffer, this);
        collector.recordStats(this, buffer.position(), 0, 0, 1, maxExceptionsNum);
    }
}
