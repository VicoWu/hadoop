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
package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.client.impl.metrics.BlockReaderIoProvider;
import org.apache.hadoop.hdfs.client.impl.metrics.BlockReaderLocalMetrics;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BlockReaderLocal enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 * {@link BlockReaderLocal} works as follows:
 * <ul>
 * <li>The client performing short circuit reads must be configured at the
 * datanode.</li>
 * <li>The client gets the file descriptors for the metadata file and the data
 * file for the block using
 * {@link org.apache.hadoop.hdfs.server.datanode.DataXceiver#requestShortCircuitFds}.
 * </li>
 * <li>The client reads the file descriptors.</li>
 * </ul>
 */
@InterfaceAudience.Private
class BlockReaderLocal implements BlockReader {
  static final Logger LOG = LoggerFactory.getLogger(BlockReaderLocal.class);

  private static final DirectBufferPool bufferPool = new DirectBufferPool();

  private static BlockReaderLocalMetrics metrics;
  private static Lock metricsInitializationLock = new ReentrantLock();
  private final BlockReaderIoProvider blockReaderIoProvider;
  private static final Timer TIMER = new Timer();

  public static class Builder {
    private final int bufferSize;
    private boolean verifyChecksum;
    private int maxReadahead;
    private String filename;
    private ShortCircuitReplica replica;
    private long dataPos;
    private ExtendedBlock block;
    private StorageType storageType;
    private ShortCircuitConf shortCircuitConf;

    public Builder(ShortCircuitConf conf) {
      this.shortCircuitConf = conf;
      this.maxReadahead = Integer.MAX_VALUE;
      this.verifyChecksum = !conf.isSkipShortCircuitChecksums();
      this.bufferSize = conf.getShortCircuitBufferSize();
    }

    public Builder setVerifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
      return this;
    }

    public Builder setCachingStrategy(CachingStrategy cachingStrategy) {
      long readahead = cachingStrategy.getReadahead() != null ?
          cachingStrategy.getReadahead() :
              HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT;
      this.maxReadahead = (int)Math.min(Integer.MAX_VALUE, readahead);
      return this;
    }

    public Builder setFilename(String filename) {
      this.filename = filename;
      return this;
    }

    public Builder setShortCircuitReplica(ShortCircuitReplica replica) {
      this.replica = replica;
      return this;
    }

    public Builder setStartOffset(long startOffset) {
      this.dataPos = Math.max(0, startOffset);
      return this;
    }

    public Builder setBlock(ExtendedBlock block) {
      this.block = block;
      return this;
    }

    public Builder setStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
    }

    public BlockReaderLocal build() {
      Preconditions.checkNotNull(replica);
      return new BlockReaderLocal(this);
    }
  }

  private boolean closed = false;

  /**
   * Pair of streams for this block.
   */
  private final ShortCircuitReplica replica;

  /**
   * The data FileChannel.
   */
  private final FileChannel dataIn;

  /**
   * The next place we'll read from in the block data FileChannel.
   *
   * If data is buffered in dataBuf, this offset will be larger than the
   * offset of the next byte which a read() operation will give us.
   */
  private long dataPos;

  /**
   * The Checksum FileChannel.
   */
  private final FileChannel checksumIn;

  /**
   * Checksum type and size.
   */
  private final DataChecksum checksum;

  /**
   * If false, we will always skip the checksum.
   */
  private final boolean verifyChecksum;

  /**
   * Name of the block, for logging purposes.
   */
  private final String filename;

  /**
   * Block ID and Block Pool ID.
   */
  private final ExtendedBlock block;

  /**
   * Cache of Checksum#bytesPerChecksum.
   */
  private final int bytesPerChecksum;

  /**
   * Cache of Checksum#checksumSize.
   */
  private final int checksumSize;

  /**
   * Maximum number of chunks to allocate.
   *
   * This is used to allocate dataBuf and checksumBuf, in the event that
   * we need them.
   */
  private final int maxAllocatedChunks;

  /**
   * True if zero readahead was requested.
   */
  private final boolean zeroReadaheadRequested;

  /**
   * Maximum amount of readahead we'll do.  This will always be at least the,
   * size of a single chunk, even if {@link #zeroReadaheadRequested} is true.
   * The reason is because we need to do a certain amount of buffering in order
   * to do checksumming.
   *
   * This determines how many bytes we'll use out of dataBuf and checksumBuf.
   * Why do we allocate buffers, and then (potentially) only use part of them?
   * The rationale is that allocating a lot of buffers of different sizes would
   * make it very difficult for the DirectBufferPool to re-use buffers.
   */
  private final int maxReadaheadLength;

  /**
   * Buffers data starting at the current dataPos and extending on
   * for dataBuf.limit().
   *
   * This may be null if we don't need it.
   */
  private ByteBuffer dataBuf;

  /**
   * Buffers checksums starting at the current checksumPos and extending on
   * for checksumBuf.limit().
   *
   * This may be null if we don't need it.
   */
  private ByteBuffer checksumBuf;

  /**
   * StorageType of replica on DataNode.
   */
  private StorageType storageType;

  private BlockReaderLocal(Builder builder) {
    this.replica = builder.replica;
    this.dataIn = replica.getDataStream().getChannel();
    this.dataPos = builder.dataPos;
    //  checksum的信息是从block meta中读取出来的
    this.checksumIn = replica.getMetaStream().getChannel();
    BlockMetadataHeader header = builder.replica.getMetaHeader();
    this.checksum = header.getChecksum();
    this.verifyChecksum = builder.verifyChecksum &&
        (this.checksum.getChecksumType().id != DataChecksum.CHECKSUM_NULL);
    this.filename = builder.filename;
    this.block = builder.block;
    // 多少数据做一次checksum，这里是一个chunk会计算一个checksum
    this.bytesPerChecksum = checksum.getBytesPerChecksum();
    // 一个checksum的数据的长度，比如crc32的checksum算法，一个checksum是4kb
    this.checksumSize = checksum.getChecksumSize();

    this.maxAllocatedChunks = (bytesPerChecksum == 0) ? 0 :
        ((builder.bufferSize + bytesPerChecksum - 1) / bytesPerChecksum);
    // Calculate the effective maximum readahead.
    // We can't do more readahead than there is space in the buffer.
    int maxReadaheadChunks = (bytesPerChecksum == 0) ? 0 :
        ((Math.min(builder.bufferSize, builder.maxReadahead) +
            bytesPerChecksum - 1) / bytesPerChecksum);
    if (maxReadaheadChunks == 0) {
      this.zeroReadaheadRequested = true;
      maxReadaheadChunks = 1;
    } else {
      this.zeroReadaheadRequested = false;
    }
    this.maxReadaheadLength = maxReadaheadChunks * bytesPerChecksum;
    this.storageType = builder.storageType;

    if (builder.shortCircuitConf.isScrMetricsEnabled()) {
      metricsInitializationLock.lock();
      try {
        if (metrics == null) {
          metrics = BlockReaderLocalMetrics.create();
        }
      } finally {
        metricsInitializationLock.unlock();
      }
    }

    this.blockReaderIoProvider = new BlockReaderIoProvider(
        builder.shortCircuitConf, metrics, TIMER);
  }

  private synchronized void createDataBufIfNeeded() {
    if (dataBuf == null) {
      dataBuf = bufferPool.getBuffer(maxAllocatedChunks * bytesPerChecksum);
      dataBuf.position(0);
      dataBuf.limit(0);
    }
  }

  private synchronized void freeDataBufIfExists() {
    if (dataBuf != null) {
      // When disposing of a dataBuf, we have to move our stored file index
      // backwards.
      dataPos -= dataBuf.remaining();
      dataBuf.clear();
      bufferPool.returnBuffer(dataBuf);
      dataBuf = null;
    }
  }

  private synchronized void createChecksumBufIfNeeded() {
    if (checksumBuf == null) {
      checksumBuf = bufferPool.getBuffer(maxAllocatedChunks * checksumSize);
      checksumBuf.position(0);
      checksumBuf.limit(0);
    }
  }

  private synchronized void freeChecksumBufIfExists() {
    if (checksumBuf != null) {
      checksumBuf.clear();
      bufferPool.returnBuffer(checksumBuf);
      checksumBuf = null; // 便于垃圾回收
    }
  }

  private synchronized int drainDataBuf(ByteBuffer buf) {
    if (dataBuf == null) return -1;
    int oldLimit = dataBuf.limit();
    int nRead = Math.min(dataBuf.remaining(), buf.remaining());
    if (nRead == 0) {
      return (dataBuf.remaining() == 0) ? -1 : 0;
    }
    try {
      dataBuf.limit(dataBuf.position() + nRead);
      buf.put(dataBuf);
    } finally {
      dataBuf.limit(oldLimit);
    }
    return nRead;
  }

  /**
   * Read from the block file into a buffer.
   *
   * This function overwrites checksumBuf.  It will increment dataPos.
   *
   * @param buf   The buffer to read into.  May be dataBuf.
   *              The position and limit of this buffer should be set to
   *              multiples of the checksum size.
   * @param canSkipChecksum  True if we can skip checksumming.
   *
   * @return      Total bytes read.  0 on EOF.
   */
  private synchronized int fillBuffer(ByteBuffer buf, boolean canSkipChecksum)
      throws IOException {
    int total = 0;
    long startDataPos = dataPos; // 读取数据的位置
    int startBufPos = buf.position(); //buf当前插入的坐标
    while (buf.hasRemaining()) {
      int nRead = blockReaderIoProvider.read(dataIn, buf, dataPos);
      if (nRead < 0) {
        break;
      }
      dataPos += nRead;
      total += nRead;
    }
    if (canSkipChecksum) { //如果可以不进行checksum校验(只是不校验checksum，不代表数据中没有携带checksum)
      freeChecksumBufIfExists();
      return total;
    }
    if (total > 0) {
      try {
        buf.limit(buf.position());
        buf.position(startBufPos);
        createChecksumBufIfNeeded();
        int checksumsNeeded = (total + bytesPerChecksum - 1) /
            bytesPerChecksum;
        checksumBuf.clear();
        checksumBuf.limit(checksumsNeeded * checksumSize);
        long checksumPos = BlockMetadataHeader.getHeaderSize()
            + ((startDataPos / bytesPerChecksum) * checksumSize);
        while (checksumBuf.hasRemaining()) {
          // 将checksum的内容(通过checksumPos这个stream中读到的)从读取到checksumBuf中，
          int nRead = checksumIn.read(checksumBuf, checksumPos);
          if (nRead < 0) {
            throw new IOException("Got unexpected checksum file EOF at " +
                checksumPos + ", block file position " + startDataPos +
                " for block " + block + " of file " + filename);
          }
          checksumPos += nRead;
        }
        checksumBuf.flip(); // 从写模式转变成读模式
        // 进行 checksum校验
        checksum.verifyChunkedSums(buf, checksumBuf, filename, startDataPos);
      } finally {
        buf.position(buf.limit());
      }
    }
    return total;
  }

  private boolean createNoChecksumContext() {
    // 客户端通过配置的方式配置为跳过checksum, 或者是临时存储， 或者，如果verifyChecksum为true，同时storagetype不是临时存储，
    // 那么添加checksum anchor计数。如果计数成功，就返回true，计数失败返回false。
    // 能否anchor，是在DataNode端通过ShortCircuitRegistry.registerSlot() -》 slot.makeAnchorable()来判断的
    return !verifyChecksum ||
        // Checksums are not stored for replicas on transient storage.  We do
        // not anchor, because we do not intend for client activity to block
        // eviction from transient storage on the DataNode side.
        (storageType != null && storageType.isTransient()) ||
        replica.addNoChecksumAnchor();
  }

  private void releaseNoChecksumContext() {
    if (verifyChecksum) {
      if (storageType == null || !storageType.isTransient()) {
        replica.removeNoChecksumAnchor();
      }
    }
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    boolean canSkipChecksum = createNoChecksumContext();
    try {
      String traceFormatStr = "read(buf.remaining={}, block={}, filename={}, "
          + "canSkipChecksum={})";
      LOG.trace(traceFormatStr + ": starting",
          buf.remaining(), block, filename, canSkipChecksum);
      int nRead;
      try {
        if (canSkipChecksum && zeroReadaheadRequested) {
          nRead = readWithoutBounceBuffer(buf);
        } else {
          nRead = readWithBounceBuffer(buf, canSkipChecksum);
        }
      } catch (IOException e) {
        LOG.trace(traceFormatStr + ": I/O error",
            buf.remaining(), block, filename, canSkipChecksum, e);
        throw e;
      }
      LOG.trace(traceFormatStr + ": returning {}",
          buf.remaining(), block, filename, canSkipChecksum, nRead);
      return nRead;
    } finally {
      if (canSkipChecksum) releaseNoChecksumContext();
    }
  }

  private synchronized int readWithoutBounceBuffer(ByteBuffer buf)
      throws IOException {
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    int total = 0;
    while (buf.hasRemaining()) {
      int nRead = blockReaderIoProvider.read(dataIn, buf, dataPos);
      if (nRead <= 0) break;
      dataPos += nRead;
      total += nRead;
    }
    return (total == 0 && (dataPos == dataIn.size())) ? -1 : total;
  }

  /**
   * Fill the data buffer.  If necessary, validate the data against the
   * checksums.
   *
   * We always want the offsets of the data contained in dataBuf to be
   * aligned to the chunk boundary.  If we are validating checksums, we
   * accomplish this by seeking backwards in the file until we're on a
   * chunk boundary.  (This is necessary because we can't checksum a
   * partial chunk.)  If we are not validating checksums, we simply only
   * fill the latter part of dataBuf.
   *
   * @param canSkipChecksum  true if we can skip checksumming.
   * @return                 true if we hit EOF.
   * @throws IOException
   */
  private synchronized boolean fillDataBuf(boolean canSkipChecksum)
      throws IOException {
    createDataBufIfNeeded();
    // 关于checksum，查看BlockSender
    // 如果bytesPerChecksum = 10,dataPos = 3, 那么slop = 3, 代表现在超出了一个chunk的坐标
    final int slop = (int)(dataPos % bytesPerChecksum);
    final long oldDataPos = dataPos;
    dataBuf.limit(maxReadaheadLength);
    if (canSkipChecksum) {
      dataBuf.position(slop); //dataBuf接着原来的位置写
      fillBuffer(dataBuf, true);
    } else { // 需要validate checksum
      dataPos -= slop;// 回到chunk的边界，即可能会重复读取一些数据
      dataBuf.position(0);//dataBuf从头开始写
      fillBuffer(dataBuf, false);
    }
    dataBuf.limit(dataBuf.position());
    dataBuf.position(Math.min(dataBuf.position(), slop));
    LOG.trace("loaded {} bytes into bounce buffer from offset {} of {}",
        dataBuf.remaining(), oldDataPos, block);
    return dataBuf.limit() != maxReadaheadLength;
  }

  /**
   * Read using the bounce buffer.
   *
   * A 'direct' read actually has three phases. The first drains any
   * remaining bytes from the slow read buffer. After this the read is
   * guaranteed to be on a checksum chunk boundary. If there are still bytes
   * to read, the fast direct path is used for as many remaining bytes as
   * possible, up to a multiple of the checksum chunk size. Finally, any
   * 'odd' bytes remaining at the end of the read cause another slow read to
   * be issued, which involves an extra copy.
   *
   * Every 'slow' read tries to fill the slow read buffer in one go for
   * efficiency's sake. As described above, all non-checksum-chunk-aligned
   * reads will be served from the slower read path.
   *
   * @param buf              The buffer to read into.
   * @param canSkipChecksum  True if we can skip checksums.
   */
  private synchronized int readWithBounceBuffer(ByteBuffer buf,
        boolean canSkipChecksum) throws IOException {
    int total = 0;
    int bb = drainDataBuf(buf); // drain bounce buffer if possible
    if (bb >= 0) {
      total += bb;
      if (buf.remaining() == 0) return total;
    }
    boolean eof = true, done = false;
    do {
      if (buf.isDirect() && (buf.remaining() >= maxReadaheadLength)
            && ((dataPos % bytesPerChecksum) == 0)) {
        // Fast lane: try to read directly into user-supplied buffer, bypassing
        // bounce buffer.
        int oldLimit = buf.limit();
        int nRead;
        try {
          buf.limit(buf.position() + maxReadaheadLength);
          nRead = fillBuffer(buf, canSkipChecksum);
        } finally {
          buf.limit(oldLimit);
        }
        if (nRead < maxReadaheadLength) {
          done = true;
        }
        if (nRead > 0) {
          eof = false;
        }
        total += nRead;
      } else {
        // Slow lane: refill bounce buffer.
        if (fillDataBuf(canSkipChecksum)) {
          done = true;
        }
        bb = drainDataBuf(buf); // drain bounce buffer if possible
        if (bb >= 0) {
          eof = false;
          total += bb;
        }
      }
    } while ((!done) && (buf.remaining() > 0));
    return (eof && total == 0) ? -1 : total;
  }

  @Override
  public synchronized int read(byte[] arr, int off, int len)
        throws IOException {
    boolean canSkipChecksum = createNoChecksumContext();
    int nRead;
    try {
      final String traceFormatStr = "read(arr.length={}, off={}, len={}, "
          + "filename={}, block={}, canSkipChecksum={})";
      LOG.trace(traceFormatStr + ": starting",
          arr.length, off, len, filename, block, canSkipChecksum);
      try {
        if (canSkipChecksum && zeroReadaheadRequested) {
          nRead = readWithoutBounceBuffer(arr, off, len);
        } else {
          nRead = readWithBounceBuffer(arr, off, len, canSkipChecksum);
        }
      } catch (IOException e) {
        LOG.trace(traceFormatStr + ": I/O error",
            arr.length, off, len, filename, block, canSkipChecksum, e);
        throw e;
      }
      LOG.trace(traceFormatStr + ": returning {}",
          arr.length, off, len, filename, block, canSkipChecksum, nRead);
    } finally {
      if (canSkipChecksum) releaseNoChecksumContext();
    }
    return nRead;
  }

  private synchronized int readWithoutBounceBuffer(byte arr[], int off,
        int len) throws IOException {
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    int nRead = blockReaderIoProvider.read(
        dataIn, ByteBuffer.wrap(arr, off, len), dataPos);
    if (nRead > 0) {
      dataPos += nRead;
    } else if ((nRead == 0) && (dataPos == dataIn.size())) {
      return -1;
    }
    return nRead;
  }

  private synchronized int readWithBounceBuffer(byte arr[], int off, int len,
        boolean canSkipChecksum) throws IOException {
    createDataBufIfNeeded();
    if (!dataBuf.hasRemaining()) { // position==limit, 已经不能再写
      dataBuf.position(0); // 重置databuf
      dataBuf.limit(maxReadaheadLength); // 再往0 ～ maxReadaheadLength中间写入数据
      fillDataBuf(canSkipChecksum);
    }
    if (dataBuf.remaining() == 0) return -1; // limit和position相同，此时不可读不可写（如果可读，肯定做了flip）
    int toRead = Math.min(dataBuf.remaining(), len);
    dataBuf.get(arr, off, toRead); // 把数据从ByteBuffer传输到arr中
    return toRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    int discardedFromBuf = 0;
    long remaining = n;
    if ((dataBuf != null) && dataBuf.hasRemaining()) {
      discardedFromBuf = (int)Math.min(dataBuf.remaining(), n);
      dataBuf.position(dataBuf.position() + discardedFromBuf);
      remaining -= discardedFromBuf;
    }
    LOG.trace("skip(n={}, block={}, filename={}): discarded {} bytes from "
            + "dataBuf and advanced dataPos by {}",
        n, block, filename, discardedFromBuf, remaining);
    dataPos += remaining;
    return n;
  }

  @Override
  public int available() {
    // We never do network I/O in BlockReaderLocal.
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    LOG.trace("close(filename={}, block={})", filename, block);
    replica.unref();
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    if (metrics != null) {
      metrics.collectThreadLocalStates();
    }
  }

  @Override
  public synchronized void readFully(byte[] arr, int off, int len)
      throws IOException {
    BlockReaderUtil.readFully(this, arr, off, len);
  }

  @Override
  public synchronized int readAll(byte[] buf, int off, int len)
      throws IOException {
    return BlockReaderUtil.readAll(this, buf, off, len);
  }

  @Override
  public boolean isShortCircuit() {
    return true;
  }

  /**
   * Get or create a memory map for this replica.
   *
   * There are two kinds of ClientMmap objects we could fetch here: one that
   * will always read pre-checksummed data, and one that may read data that
   * hasn't been checksummed.
   *
   * If we fetch the former, "safe" kind of ClientMmap, we have to increment
   * the anchor count on the shared memory slot.  This will tell the DataNode
   * not to munlock the block until this ClientMmap is closed.
   * If we fetch the latter, we don't bother with anchoring.
   *
   *
   * @param opts     The options to use, such as SKIP_CHECKSUMS.
   *
   * @return         null on failure; the ClientMmap otherwise.
   */
  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    boolean anchor = verifyChecksum &&
        !opts.contains(ReadOption.SKIP_CHECKSUMS);
    if (anchor) { // 需要进行数据校验
      if (!createNoChecksumContext()) { // 创建unanchor_flag失败
        LOG.trace("can't get an mmap for {} of {} since SKIP_CHECKSUMS was not "
            + "given, we aren't skipping checksums, and the block is not "
            + "mlocked.", block, filename);
        return null;
      }
    }
    ClientMmap clientMmap = null;
    try {
      clientMmap = replica.getOrCreateClientMmap(anchor);
    } finally {
      if ((clientMmap == null) && anchor) {
        releaseNoChecksumContext();
      }
    }
    return clientMmap;
  }

  @VisibleForTesting
  boolean getVerifyChecksum() {
    return this.verifyChecksum;
  }

  @VisibleForTesting
  int getMaxReadaheadLength() {
    return this.maxReadaheadLength;
  }

  /**
   * Make the replica anchorable.  Normally this can only be done by the
   * DataNode.  This method is only for testing.
   */
  @VisibleForTesting
  void forceAnchorable() {
    replica.getSlot().makeAnchorable();
  }

  /**
   * Make the replica unanchorable.  Normally this can only be done by the
   * DataNode.  This method is only for testing.
   */
  @VisibleForTesting
  void forceUnanchorable() {
    replica.getSlot().makeUnanchorable();
  }

  @Override
  public DataChecksum getDataChecksum() {
    return checksum;
  }

  @Override
  public int getNetworkDistance() {
    return 0;
  }
}
