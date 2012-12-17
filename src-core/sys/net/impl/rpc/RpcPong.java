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
package sys.net.impl.rpc;

import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RpcPong extends RpcEcho implements KryoSerializable {

    public long departure_ts;
    public long arrival_ts;

    public RpcPong() {
    }

    public RpcPong(RpcPing other) {
        this.departure_ts = other.departure_ts;
    }

    public double rtt() {
        return arrival_ts - departure_ts;
    }

    @Override
    public void deliverTo(TransportConnection conn, MessageHandler handler) {
        ((RpcEchoHandler) handler).onReceive(conn, this);
    }

    @Override
    public void read(Kryo kryo, Input in) {
        departure_ts = in.readLong();
        arrival_ts = System.currentTimeMillis();
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeLong(departure_ts);
    }
}
