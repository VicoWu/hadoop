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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.tracing.TraceScope;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends OutputStream implements
    StreamCapabilities {
  // data checksum
  private final DataChecksum sum;
  // internal buffer for storing data before it is checksumed
  private byte buf[];
  // internal buffer for storing checksum
  private byte checksum[];
  // The number of valid bytes in the buffer.
  private int count;
  
  // We want this value to be a multiple of 3 because the native code checksums
  // 3 chunks simultaneously. The chosen value of 9 strikes a balance between
  // limiting the number of JNI calls and flushing to the underlying stream
  // relatively frequently.
  private static final int BUFFER_NUM_CHUNKS = 9;
  
  protected FSOutputSummer(DataChecksum sum) {
    this.sum = sum;
    this.buf = new byte[sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS];
    this.checksum = new byte[getChecksumSize() * BUFFER_NUM_CHUNKS];
    this.count = 0;
  }
  
  /* write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len > 0</code>, and its checksum
   */
  protected abstract void writeChunk(byte[] b, int bOffset, int bLen,
      byte[] checksum, int checksumOffset, int checksumLen) throws IOException;
  
  /**
   * Check if the implementing OutputStream is closed and should no longer
   * accept writes. Implementations should do nothing if this stream is not
   * closed, and should throw an {@link IOException} if it is closed.
   * 
   * @throws IOException if this stream is already closed.
   */
  protected abstract void checkClosed() throws IOException;

  /** Write one byte */
  @Override
  public synchronized void write(int b) throws IOException {
    buf[count++] = (byte)b;
    if(count == buf.length) {
      flushBuffer();
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array 
   * starting at offset <code>off</code> and generate a checksum for
   * each data chunk.
   *
   * <p> This method stores bytes from the given array into this
   * stream's buffer before it gets checksumed. The buffer gets checksumed 
   * and flushed to the underlying output stream when all data 
   * in a checksum chunk are in the buffer.  If the buffer is empty and
   * requested length is at least as large as the size of next checksum chunk
   * size, this method will checksum and write the chunk directly 
   * to the underlying output stream.  Thus it avoids unnecessary data copy.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public synchronized void write(byte b[], int off, int len)
      throws IOException {
    
    checkClosed();
    
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    // write1返回的是每次写成功的字节数
    for (int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
  }
  
  /**
   * Write a portion of an array, flushing to the underlying
   * stream at most once if necessary.
   * 返回拷贝成功的数据量（因为buf可能不够）
   * 将b[]中从[off, off+len]中的数据写入到buf中
   */
  private int write1(byte b[], int off, int len) throws IOException {
    if(count==0 && len>=buf.length) { // 如果buf中没有数据，并且当前写入数据的长度大于buf，那么，不需要经过buf，直接开始生成chunk
      // local buffer is empty and user buffer size >= local buffer size, so
      // simply checksum the user buffer and send it directly to the underlying
      // stream
      final int length = buf.length;
      writeChecksumChunks(b, off, length);// 最终调用DFSStripedOutputStream或者DFSOutputStream的writeChecksumChunks
      return length;
    }
    
    // copy user data to local buffer
    int bytesToCopy = buf.length-count; // 当前buf中剩余的字节数
    // 如果len小于剩余可写字节数，那么就可以全部拷贝到buf中，否则，只能将buf拷贝满，但是还有数据没有拷贝进来
    bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
    System.arraycopy(b, off, buf, count, bytesToCopy);
    count += bytesToCopy;
    if (count == buf.length) { // buf满了
      // local buffer is full
      flushBuffer();
    } 
    return bytesToCopy;
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream. 
   */
  protected synchronized void flushBuffer() throws IOException {
    flushBuffer(false, true);
  }

  /* Forces buffered output bytes to be checksummed and written out to
   * the underlying output stream. If there is a trailing partial chunk in the
   * buffer,
   * 1) flushPartial tells us whether to flush that chunk
   * 2) if flushPartial is true, keep tells us whether to keep that chunk in the
   * buffer (if flushPartial is false, it is always kept in the buffer)
   *
   * Returns the number of bytes that were flushed but are still left in the
   * buffer (can only be non-zero if keep is true).
   * 将buf中的数据进行flush，有可能buf中的数据不是一个chunk的整数长度倍，如果存在这种情况，并且flushPartial=false, 那么就需要截取出来
   */
  protected synchronized int flushBuffer(boolean keep,
      boolean flushPartial) throws IOException {
    int bufLen = count;
    int partialLen = bufLen % sum.getBytesPerChecksum(); // 对每次需要计算checksum的数据量取余操作
    int lenToFlush = flushPartial ? bufLen : bufLen - partialLen;
    if (lenToFlush != 0) {
      writeChecksumChunks(buf, 0, lenToFlush);
      if (!flushPartial || keep) {
        count = partialLen; // 剩余的数据
        System.arraycopy(buf, bufLen - count, buf, 0, count);// 把剩余的数据重新从头放到buf中
      } else {
        count = 0;
      }
    }

    // total bytes left minus unflushed bytes left
    return count - (bufLen - lenToFlush);
  }

  /**
   * Checksums all complete data chunks and flushes them to the underlying
   * stream. If there is a trailing partial chunk, it is not flushed and is
   * maintained in the buffer.
   */
  public void flush() throws IOException {
    flushBuffer(false, false);
  }

  /**
   * Return the number of valid bytes currently in the buffer.
   */
  protected synchronized int getBufferedDataSize() {
    return count;
  }

  /** @return the size for a checksum. */
  protected int getChecksumSize() {
    return sum.getChecksumSize();
  }

  protected DataChecksum getDataChecksum() {
    return sum;
  }

  protected TraceScope createWriteTraceScope() {
    return null;
  }

  /** Generate checksums for the given data chunks and output chunks & checksums
   * to the underlying output stream.
   * 这个b[]来自于OutputSummer的buf
   */
  private void writeChecksumChunks(byte b[], int off, int len)
  throws IOException {
    // 计算b的checksum，每个chunk的checksum占用4个byte
    sum.calculateChunkedSums(b, off, len, checksum, 0);
    TraceScope scope = createWriteTraceScope();
    try {
      for (int i = 0; i < len; i += sum.getBytesPerChecksum()) { // 每次写一个chunk的数据
        int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i); // 如果剩余的数据不足一个chunk，就写全部的剩余
        int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize(); // 普通的CRC校验是4byte，ckOffset指的是在checksum结果数组中的校验码
        writeChunk(b, off + i, chunkLen, checksum, ckOffset,
            getChecksumSize());
      }
    } finally {
      if (scope != null) {
        scope.close();
      }
    }
  }

  /**
   * Converts a checksum integer value to a byte stream
   */
  static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
    return int2byte((int)sum.getValue(), new byte[checksumSize]);
  }

  static byte[] int2byte(int integer, byte[] bytes) {
    if (bytes.length != 0) {
      bytes[0] = (byte) ((integer >>> 24) & 0xFF);
      bytes[1] = (byte) ((integer >>> 16) & 0xFF);
      bytes[2] = (byte) ((integer >>> 8) & 0xFF);
      bytes[3] = (byte) ((integer >>> 0) & 0xFF);
      return bytes;
    }
    return bytes;
  }

  /**
   * Resets existing buffer with a new one of the specified size.
   */
  protected synchronized void setChecksumBufSize(int size) {
    this.buf = new byte[size];
    this.checksum = new byte[sum.getChecksumSize(size)];
    this.count = 0;
  }

  protected synchronized void resetChecksumBufSize() {
    setChecksumBufSize(sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS);
  }

  @Override
  public boolean hasCapability(String capability) {
    return false;
  }
}
