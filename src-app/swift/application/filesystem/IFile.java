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
     * Returns the content of specified from the offset as byte buffer.
     * 
     * @param offset
     * @param length
     * @return
     */
    byte[] get(int offset, int length);

    /**
     * Returns content as byte array.
     * 
     * @return
     */
    byte[] getBytes();

    /**
     * Returns the size of the file in bytes.
     * 
     * @return
     */
    int getSize();

}
