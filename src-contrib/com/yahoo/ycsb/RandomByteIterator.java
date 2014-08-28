/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *                                                              
 * http://www.apache.org/licenses/LICENSE-2.0
 *                                                            
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb;

/**
 *  A ByteIterator that generates a random sequence of bytes.
 */
public class RandomByteIterator extends ByteIterator {
  private long len;
  private long off;     // global offset
  private int bufOff;   // offset in the buffer
  private byte[] buf;
  private boolean clean = true;

  @Override
  public boolean hasNext() {
    return off < len;
  }

  private void fillBytesImpl(byte[] buffer, int base) {
    int bytes = Utils.random().nextInt();
    try {
      buffer[base+0] = (byte)(((bytes) & 31) + ' ');
      buffer[base+1] = (byte)(((bytes >> 5) & 31) + ' ');
      buffer[base+2] = (byte)(((bytes >> 10) & 31) + ' ');
      buffer[base+3] = (byte)(((bytes >> 15) & 31) + ' ');
      buffer[base+4] = (byte)(((bytes >> 20) & 31) + ' ');
      buffer[base+5] = (byte)(((bytes >> 25) & 31) + ' ');
    } catch (ArrayIndexOutOfBoundsException e) { /* ignore it */ }
  }

  private void fillDateImpl(byte[] buffer, int base) {
      try {
        buffer[base+0] = (byte)(((ts) & 31) + ' ');
        buffer[base+1] = (byte)(((ts >> 5) & 31) + ' ');
        buffer[base+2] = (byte)(((ts >> 10) & 31) + ' ');
        buffer[base+3] = (byte)(((ts >> 15) & 31) + ' ');
        buffer[base+4] = (byte)(((ts >> 20) & 31) + ' ');
        buffer[base+5] = (byte)(((ts >> 25) & 31) + ' ');
      } catch (ArrayIndexOutOfBoundsException e) { /* ignore it */ }
    }

  private void fillBytes() {
    if(bufOff ==  buf.length) {
      fillBytesImpl(buf, 0);
      bufOff = 0;
    }
  }
  
  private void fillDate() {
      if(bufOff ==  buf.length) {
        fillDateImpl(buf, 0);
        bufOff = 0;
      }
    }
    

  public RandomByteIterator(long len) {
    this.len = len;
    this.buf = new byte[6];
    this.bufOff = 6;
    this.off = 0;
  }

  protected byte nextByte0() {
      if( super.read == 0)
          fillDate();
      else
          fillBytes();
    off++;
    bufOff++;
    return buf[bufOff-1];
  }

  @Override
  public int nextBuf(byte[] buffer, int bufferOffset) {
    int ret;
    if(len - off < buffer.length - bufferOffset) {
      ret = (int)(len - off);
    } else {
      ret = buffer.length - bufferOffset;
    }
    int i;
    for(i = 0; i < ret; i+=6) {
      fillBytesImpl(buffer, i + bufferOffset);
    }
    off+=ret;
    return ret + bufferOffset;
  }

  @Override
  protected long bytesLeft0() {
    return len - off;
  }
}
