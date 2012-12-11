/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 University of Kaiserslautern
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FilePaged implements IFile {
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private List<byte[]> pages;
    // size of file (in bytes)
    private long actualSize;

    public FilePaged(final Blob initial) {
        pages = new ArrayList<byte[]>();
        fill(initial.get());
    }

    private int pageIndex(final long offset) {
        return (int) (offset / DEFAULT_PAGE_SIZE);
    }

    private int inPageOffset(final long offset) {
        return (int) (offset % DEFAULT_PAGE_SIZE);
    }

    private void allocatePagesForIndex(final int requiredIndex) {
        for (int index = pages.size(); index <= requiredIndex; ++index) {
            pages.add(new byte[DEFAULT_PAGE_SIZE]);
        }
    }

    @Override
    public void update(final ByteBuffer buf, final long offset) {
        long newSize = offset + buf.remaining();
        int endIndex = pageIndex(newSize);
        allocatePagesForIndex(endIndex);

        int index = pageIndex(offset);
        int delta = inPageOffset(offset);

        while (buf.remaining() > 0) {
            byte[] target = pages.get(index);
            int pageRemaining = target.length - delta;
            assert pageRemaining > 0;
            int size = Math.min(buf.remaining(), pageRemaining);
            buf.get(target, delta, size);
            index++;
            delta = 0;
        }
        actualSize = Math.max(actualSize, newSize);
    }

    @Override
    public void reset(final byte[] data) throws IOException {
        fill(data);
    }

    private void fill(final byte[] data) {
        actualSize = 0;
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            update(buf, buf.position());
        }
    }

    @Override
    public void read(final ByteBuffer buf, final long offset) {
        assert offset + buf.remaining() >= actualSize;

        int index = pageIndex(offset);
        int delta = inPageOffset(offset);

        while (buf.remaining() > 0) {
            byte[] target = pages.get(index);
            int pageRemaining = target.length - delta;
            assert pageRemaining > 0;
            int size = Math.min(buf.remaining(), pageRemaining);
            buf.put(target, delta, size);
            index++;
            delta = 0;
        }
    }

    @Override
    public byte[] get(final int offset, final int length) {
        ByteBuffer buf = ByteBuffer.allocate(length);
        while (buf.remaining() > 0) {
            read(buf, offset);
        }

        return buf.array();
    }

    @Override
    public byte[] getBytes() {
        assert actualSize <= Integer.MAX_VALUE;
        return get(0, (int) actualSize);
    }

    @Override
    public int getSize() {
        assert actualSize <= Integer.MAX_VALUE;
        return (int) actualSize;
    }
}
