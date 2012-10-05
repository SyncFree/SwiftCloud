package swift.application.filesystem;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileBasic implements IFile {
    public final int MAX_SIZE = 512;
    public int size;
    private ByteBuffer bb;

    public FileBasic(Blob b) throws IOException {
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
    public void reset(byte[] data) throws IOException {
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
