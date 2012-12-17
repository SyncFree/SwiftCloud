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
package sys.net.impl.providers;

import static sys.Sys.Sys;
import static sys.net.impl.NetworkingConstants.KRYOBUFFER_INITIAL_CAPACITY;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.logging.Logger;

import sys.net.impl.KryoLib;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class KryoOutputBuffer implements Runnable {

    private static Logger Log = Logger.getLogger(KryoOutputBuffer.class.getName());

    final Kryo kryo = KryoLib.kryo();

    Output out;
    ByteBuffer buffer;

    Runnable handler;

    public KryoOutputBuffer() {
        buffer = ByteBuffer.allocate(KRYOBUFFER_INITIAL_CAPACITY);
        out = new Output(buffer.array(), Integer.MAX_VALUE);
    }

    public void run() {
    }

    final public int writeClassAndObjectFrame(Object object, WritableByteChannel ch) throws IOException {
        out.setPosition(4);
        kryo.writeClassAndObject(out, object);
        int length = out.position();

        if (length > buffer.capacity())
            buffer = ByteBuffer.wrap(out.getBuffer());

        buffer.clear();
        buffer.putInt(0, length - 4);
        buffer.limit(length);

        int n = ch.write(buffer);
        Sys.uploadedBytes.addAndGet(n);
        return n;
    }

    final public int writeClassAndObjectFrame(Object object) throws IOException {
        out.setPosition(4);
        kryo.writeClassAndObject(out, object);
        int length = out.position();

        if (length > buffer.capacity())
            buffer = ByteBuffer.wrap(out.getBuffer());

        buffer.clear();
        buffer.putInt(0, length - 4);
        buffer.limit(length);

        Sys.uploadedBytes.addAndGet(length);
        return length;
    }

    final public int writeClassAndObject(Object object) throws IOException {
        out.setPosition(0);
        kryo.writeClassAndObject(out, object);
        int length = out.position();

        if (length > buffer.capacity())
            buffer = ByteBuffer.wrap(out.getBuffer());

        out.flush();
        return length;
    }

    final public int writeClassAndObjectFrame(Object object, DataOutputStream dos) throws IOException {
        out.clear();
        kryo.writeClassAndObject(out, object);
        int length = out.position();

        if (length > buffer.capacity())
            buffer = ByteBuffer.wrap(out.getBuffer());

        dos.writeInt(length);
        dos.write(buffer.array(), 0, length);
        return length;
    }

    public ByteBuffer toByteBuffer() {
        return buffer;
    }

    public byte[] toByteArray() {
        byte[] res = new byte[buffer.limit()];
        System.arraycopy(buffer.array(), 0, res, 0, res.length);
        return res;
    }
}