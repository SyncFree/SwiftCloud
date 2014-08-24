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
package swift.crdt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import swift.crdt.core.CRDTUpdate;

public class BloatedIntegerUpdate implements CRDTUpdate<BloatedIntegerCRDT>, KryoSerializable {
    protected String clientId;
    protected int value;
    protected boolean positive;

    // required for kryo
    public BloatedIntegerUpdate() {
    }

    public BloatedIntegerUpdate(String clientId, int val, boolean positive) {
        this.clientId = clientId;
        this.value = val;
        this.positive = positive;
    }

    public int getVal() {
        return this.value;
    }

    @Override
    public void applyTo(BloatedIntegerCRDT crdt) {
        crdt.applyClientUpdate(clientId, value, positive);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return value;
    }

    @Override
    public void read(Kryo kryo, Input in) {
        clientId = in.readString();
        positive = in.readBoolean();
        value = in.readVarInt(true);
    }

    @Override
    public void write(Kryo kryo, Output out) {
        out.writeAscii(clientId);
        out.writeBoolean(positive);
        out.writeVarInt(value, true);
    }
}
