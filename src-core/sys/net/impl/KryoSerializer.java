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
package sys.net.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

import sys.net.api.Serializer;
import sys.net.api.SerializerException;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer implements Serializer {

    private static Logger Log = Logger.getLogger(KryoSerializer.class.getName());

    public KryoSerializer() {
    }

    @Override
    public byte[] writeObject(Object obj) throws SerializerException {
        try {

            Output output = new Output(1 << 10, 1 << 22);
            KryoLib.kryo().writeClassAndObject(output, obj);
            output.close();
            return output.toBytes();
        } catch (Exception e) {
            throw new SerializerException(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(byte[] data) throws SerializerException {
        try {
            return (T) KryoLib.kryo().readClassAndObject(new Input(data));
        } catch (Exception e) {
            throw new SerializerException(e.getMessage());
        }
    }

    @Override
    public void writeObject(DataOutputStream out, Object obj) throws SerializerException {
        try {
            byte[] bytes = writeObject(obj);
            out.writeInt(bytes.length);
            out.write(bytes);
        } catch (IOException e) {
            Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
            throw new SerializerException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(DataInputStream in) throws SerializerException {
        try {
            in.readInt();
            return (T) KryoLib.kryo().readClassAndObject(new Input(in));
        } catch (IOException e) {
            Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
            throw new SerializerException(e);
        }
    }
}
