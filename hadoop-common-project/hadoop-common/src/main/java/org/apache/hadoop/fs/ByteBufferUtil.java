/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ByteBufferUtil {

  /**
   * Determine if a stream can do a byte buffer read via read(ByteBuffer buf)
   */
  private static boolean streamHasByteBufferRead(InputStream stream) {
    // 如果stream的类型不是ByteBufferReadable， 那么肯定不是ByteBufferRead
    if (!(stream instanceof ByteBufferReadable)) {
      return false;
    }
    // stream是ByteBufferReadable，但是不是FSDataInputStream
    //  注意， FSDataInputStream implements ByteBufferReadable
    if (!(stream instanceof FSDataInputStream)) { // 如果stream的类型不是FSDataInputStream， 那么肯定是ByteBufferRead
      return true;
    }
    // stream是FSDataInputStream, 底层包裹的stream是ByteBufferReadable就返回true，否则返回false
    return ((FSDataInputStream)stream).getWrappedStream() 
        instanceof ByteBufferReadable;
  }

  /**
   * Perform a fallback read.
   */
  public static ByteBuffer fallbackRead(
      InputStream stream, ByteBufferPool bufferPool, int maxLength)
          throws IOException {
    if (bufferPool == null) {
      throw new UnsupportedOperationException("zero-copy reads " +
          "were not available, and you did not provide a fallback " +
          "ByteBufferPool.");
    }
    boolean useDirect = streamHasByteBufferRead(stream);
    ByteBuffer buffer = bufferPool.getBuffer(useDirect, maxLength);
    if (buffer == null) {
      throw new UnsupportedOperationException("zero-copy reads " +
          "were not available, and the ByteBufferPool did not provide " +
          "us with " + (useDirect ? "a direct" : "an indirect") +
          "buffer.");
    }
    Preconditions.checkState(buffer.capacity() > 0);
    Preconditions.checkState(buffer.isDirect() == useDirect);
    maxLength = Math.min(maxLength, buffer.capacity());
    boolean success = false;
    try {
      if (useDirect) { // 是ByteBuffer的stream，直接读取到ByteBuffer里面去
        buffer.clear();
        buffer.limit(maxLength);
        ByteBufferReadable readable = (ByteBufferReadable)stream;
        int totalRead = 0;
        while (true) {
          if (totalRead >= maxLength) {
            success = true;
            break;
          }
          int nRead = readable.read(buffer);
          if (nRead < 0) {
            if (totalRead > 0) {
              success = true;
            }
            break;
          }
          totalRead += nRead;
        }
        buffer.flip();
      } else {  // 不是ByteBuffer的stream，读取到array中取
        buffer.clear();
        int nRead = stream.read(buffer.array(),
            buffer.arrayOffset(), maxLength);
        if (nRead >= 0) {
          buffer.limit(nRead);
          success = true;
        }
      }
    } finally {
      if (!success) {
        // If we got an error while reading, or if we are at EOF, we 
        // don't need the buffer any more.  We can give it back to the
        // bufferPool.
        bufferPool.putBuffer(buffer);
        buffer = null;
      }
    }
    return buffer;
  }
}
