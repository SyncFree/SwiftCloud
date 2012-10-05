package swift.application.filesystem;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileBasic implements IFile {
    public final int SIZE = 1024;
    private ByteBuffer bb;

    public FileBasic(Blob b) throws IOException {
        byte[] init = b.get();
        bb = ByteBuffer.allocate(SIZE);
        bb.put(init);
    }

    @Override
    public void update(ByteBuffer buf, long offset) {
        int size = buf.remaining();
        byte[] arr = new byte[size];
        buf.get(arr);

        bb.position((int) offset);
        bb.put(arr);
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
    }

    @Override
    public byte[] getBytes() {
        return bb.array();
    }

}
