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
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import sys.net.api.Serializer;
import sys.net.api.SerializerException;
import sys.net.impl.providers.KryoInputBuffer;
import sys.net.impl.providers.KryoOutputBuffer;

public class KryoSerializer implements Serializer {

    private static Logger Log = Logger.getLogger(KryoSerializer.class.getName());

    private KryoInputBuffer input;
    private KryoOutputBuffer output;

    public KryoSerializer() {
        input = new KryoInputBuffer();
        output = new KryoOutputBuffer();
    }

    @Override
    synchronized public byte[] writeObject(Object o) throws SerializerException {
        try {
            output.writeClassAndObject(o);
            return output.toByteArray();
        } catch (IOException e) {
            throw new SerializerException(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    synchronized public <T> T readObject(byte[] data) throws SerializerException {
        return (T) input.readClassAndObject(ByteBuffer.wrap(data));
    }

    @Override
    synchronized public void writeObject(DataOutputStream out, Object o) throws SerializerException {
        try {
            output.writeClassAndObjectFrame(o, out);
        } catch (IOException e) {
            Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
            throw new SerializerException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    synchronized public <T> T readObject(DataInputStream in) throws SerializerException {
        try {
            input.readFrom(in);
            return (T) input.readClassAndObject();
        } catch (IOException e) {
            Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
            throw new SerializerException(e);
        }
    }
}
