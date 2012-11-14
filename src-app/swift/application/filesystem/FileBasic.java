/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 Kaiserslautern University of Technology
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
package swift.application.filesystem;

import java.nio.ByteBuffer;

public class FileBasic implements IFile {
    public final int MAX_SIZE = 1024 * 1024 * 1;
    // 1MB for the moment to get the Andrew benchmark running
    public int size;
    private ByteBuffer bb;

    public FileBasic() {
        bb = ByteBuffer.allocate(MAX_SIZE);
        size = 0;
    }

    public FileBasic(Blob b) {
        byte[] init = b.get();
        bb = ByteBuffer.allocate(MAX_SIZE);
        bb.put(init);
        size = init.length;
    }

    @Override
    public void update(ByteBuffer buf, long offset) {
        int updateSize = buf.remaining();
        byte[] arr = new byte[updateSize];
        buf.get(arr);

        bb.position((int) offset);
        bb.put(arr);
        size = Math.max(updateSize, size);
    }

    @Override
    public void read(ByteBuffer buf, long offset) {
        bb.position((int) offset);
        byte[] arr = new byte[buf.remaining()];
        bb.get(arr);
        buf.put(arr);
    }

    @Override
    public byte[] get(int offset, int length) {
        bb.position(offset);
        byte[] bytes = new byte[length];
        bb.get(bytes, 0, length);
        return bytes;
    }

    @Override
    public void reset(byte[] data) {
        bb.clear();
        bb.put(data);
        size = data.length;
    }

    @Override
    public byte[] getBytes() {
        System.out.println("Current size of file: " + size);

        byte[] bytes = new byte[size];
        bb.position(0);
        bb.get(bytes, 0, size);
        System.out.println("Current content of file: " + new String(bytes));

        return bytes;
    }

    @Override
    public int getSize() {
        return size;
    }

}
