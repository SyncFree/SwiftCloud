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
package swift.application.swiftdoc;

import java.util.concurrent.atomic.AtomicLong;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TextLine implements KryoSerializable {

    static AtomicLong g_serial = new AtomicLong(0);

    long serial;
    String text;
    long arrival_ts;
    long departure_ts;
    int commit;

    // for kryo
    TextLine() {
    }

    public TextLine(String text) {
        this.text = text;
        this.serial = g_serial.getAndIncrement();
        this.departure_ts = -1;
    }

    public TextLine(String text, int commit) {
        this.text = text;
        this.serial = g_serial.getAndIncrement();
        this.departure_ts = -1;
        this.commit = commit;
    }

    public int hashCode() {
        return text.hashCode();
    }

    public boolean equals(Object other) {
        if (other instanceof String)
            return text.equals(other);
        else
            return other instanceof TextLine && text.equals(((TextLine) other).text);
    }

    @Override
    public void read(Kryo kryo, Input in) {
        commit = in.readInt();
        serial = in.readLong();
        text = in.readString();
        departure_ts = in.readLong();
        arrival_ts = System.currentTimeMillis();
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeInt(commit);
        out.writeLong(serial);
        out.writeString(text);
        out.writeLong(departure_ts >= 0 ? departure_ts : (departure_ts = System.currentTimeMillis()));
    }

    public long latency() {
        return (arrival_ts - departure_ts) / 2;
    }

    public String toString() {
        return text;
    }

    public Long serial() {
        return serial;
    }
}
