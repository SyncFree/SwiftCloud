package swift.application.filesystem;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface IFile {
    /**
     * Gets the data from the buffer and stores it at the offset.
     * 
     * @param buf
     * @param offset
     */
    void update(ByteBuffer buf, long offset);

    /**
     * Updates the content of the file with the data from the byte array.
     * 
     * @throws IOException
     */
    void reset(byte[] data) throws IOException;

    /**
     * Fills the buffer with data from the offset.
     * 
     * @param buf
     * @param offset
     */
    void read(ByteBuffer buf, long offset);

    /**
     * Returns content as byte array.
     * 
     * @return
     */
    byte[] get(int offset, int length);

    byte[] getBytes();

}
