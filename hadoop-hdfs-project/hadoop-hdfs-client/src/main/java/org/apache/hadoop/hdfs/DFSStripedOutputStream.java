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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.tracing.TraceScope;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * This class supports writing files in striped layout and erasure coded format.
 * Each stripe contains a sequence of cells.
 * 虽然一个DFSStripedOutputStream对应了多个DataStreamer, 但是它始终通过streamer和currentPacket来标记当前正在写的packet和负责这个packet的streamer。因此可以看到，
 * 虽然一个Packet是64KB, 然后一个cell可能比Packet小，但是，一个Packet作为最小的发送单元，这个Packet只有可能是属于某一个internal block,不可能跨域internal block,否则
 * 这个packet 是属于不同的DN的
 */
@InterfaceAudience.Private
public class DFSStripedOutputStream extends DFSOutputStream
    implements StreamCapabilities {
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  /**
   * OutputStream level last exception, will be used to indicate the fatal
   * exception of this stream, i.e., being aborted.
   */
  private final ExceptionLastSeen exceptionLastSeen = new ExceptionLastSeen();

  static class MultipleBlockingQueue<T> {
    private final List<BlockingQueue<T>> queues;

    MultipleBlockingQueue(int numQueue, int queueSize) {
      queues = new ArrayList<>(numQueue);
      for (int i = 0; i < numQueue; i++) {
        queues.add(new LinkedBlockingQueue<T>(queueSize));
      }
    }

    void offer(int i, T object) { // 向index = i的BlockingQueue中添加一个LocatedBlock(internal block)
      final boolean b = queues.get(i).offer(object);
      Preconditions.checkState(b, "Failed to offer " + object
          + " to queue, i=" + i);
    }

    T take(int i) throws InterruptedIOException {
      try {
        return queues.get(i).take();
      } catch(InterruptedException ie) {
        throw DFSUtilClient.toInterruptedIOException("take interrupted, i=" + i, ie);
      }
    }

    T takeWithTimeout(int i) throws InterruptedIOException {
      try {
        return queues.get(i).poll(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw DFSUtilClient.toInterruptedIOException("take interrupted, i=" + i, e);
      }
    }

    T poll(int i) {
      return queues.get(i).poll();
    }

    T peek(int i) {
      return queues.get(i).peek();
    }

    void clear() {
      for (BlockingQueue<T> q : queues) {
        q.clear();
      }
    }
  }

  /** Coordinate the communication between the streamers. */
  static class Coordinator {
    /**
     * The next internal block to write to for each streamers. The
     * DFSStripedOutputStream makes the {@link ClientProtocol#addBlock} RPC to
     * get a new block group. The block group is split to internal blocks, which
     * are then distributed into the queue for streamers to retrieve.
     * followingBlocks是一个List<BlockingQueue<T>> queues，每一个BlockingQueue代表index=i的多个LocatedBlock
     */
    private final MultipleBlockingQueue<LocatedBlock> followingBlocks; // 指的是block group的下一个internal block， 每一个index对应了一系列block的list
    /**
     * Used to sync among all the streamers before allocating a new block. The
     * DFSStripedOutputStream uses this to make sure every streamer has finished
     * writing the previous block.
     */
    private final MultipleBlockingQueue<ExtendedBlock> endBlocks;

    /**
     * The following data structures are used for syncing while handling errors
     */
    private final MultipleBlockingQueue<LocatedBlock> newBlocks;
    private final Map<StripedDataStreamer, Boolean> updateStreamerMap;
    private final MultipleBlockingQueue<Boolean> streamerUpdateResult;

    Coordinator(final int numAllBlocks) { // 这里的numAllBlocks指的是一个logical Block的internal block的数量
      followingBlocks = new MultipleBlockingQueue<>(numAllBlocks, 1);
      endBlocks = new MultipleBlockingQueue<>(numAllBlocks, 1);
      newBlocks = new MultipleBlockingQueue<>(numAllBlocks, 1);
      updateStreamerMap = new ConcurrentHashMap<>(numAllBlocks);
      streamerUpdateResult = new MultipleBlockingQueue<>(numAllBlocks, 1);
    }

    MultipleBlockingQueue<LocatedBlock> getFollowingBlocks() {
      return followingBlocks;
    }

    MultipleBlockingQueue<LocatedBlock> getNewBlocks() {
      return newBlocks;
    }

    void offerEndBlock(int i, ExtendedBlock block) {
      endBlocks.offer(i, block);
    }

    void offerStreamerUpdateResult(int i, boolean success) {
      streamerUpdateResult.offer(i, success);
    }

    boolean takeStreamerUpdateResult(int i) throws InterruptedIOException {
      return streamerUpdateResult.take(i);
    }

    void updateStreamer(StripedDataStreamer streamer,
        boolean success) {
      assert !updateStreamerMap.containsKey(streamer);
      updateStreamerMap.put(streamer, success);
    }

    void clearFailureStates() {
      newBlocks.clear();
      updateStreamerMap.clear();
      streamerUpdateResult.clear();
    }
  }

  /** Buffers for writing the data and parity cells of a stripe. */
  class CellBuffers {
    private final ByteBuffer[] buffers;
    private final byte[][] checksumArrays;

    CellBuffers(int numParityBlocks) {
      if (cellSize % bytesPerChecksum != 0) {
        throw new HadoopIllegalArgumentException("Invalid values: "
            + HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (="
            + bytesPerChecksum + ") must divide cell size (=" + cellSize + ").");
      }

      checksumArrays = new byte[numParityBlocks][];
      final int size = getChecksumSize() * (cellSize / bytesPerChecksum);
      for (int i = 0; i < checksumArrays.length; i++) {
        checksumArrays[i] = new byte[size];
      }

      buffers = new ByteBuffer[numAllBlocks];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(useDirectBuffer(), cellSize);
        buffers[i].limit(cellSize);
      }
    }

    private ByteBuffer[] getBuffers() {
      return buffers;
    }

    byte[] getChecksumArray(int i) {
      return checksumArrays[i - numDataBlocks];
    }

    /**
     * 将byte[] b中的数据写入到i对应的 buffer中去
     * @param i
     * @param b
     * @param off
     * @param len
     * @return
     */
    private int addTo(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      return pos; // 返回数据写完以后新的position
    }

    private void clear() {
      for (int i = 0; i< numAllBlocks; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
      }
    }

    private void release() {
      for (int i = 0; i < numAllBlocks; i++) {
        if (buffers[i] != null) {
          BUFFER_POOL.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < numDataBlocks; i++) {
        buffers[i].flip();
      }
    }
  }

  private final Coordinator coordinator;
  private final CellBuffers cellBuffers;// cellbuffers中的ByteBuffer[] 为每一个index构建一个ByteBuffer
  private final ErasureCodingPolicy ecPolicy;
  private final RawErasureEncoder encoder;
  private final List<StripedDataStreamer> streamers;
  private final DFSPacket[] currentPackets; // current Packet of each streamer

  // Size of each striping cell, must be a multiple of bytesPerChecksum.
  // bytesPerChecksum表示做一个checksum的数据单位的长度，cellsize必须是它的整数倍
  private final int cellSize;
  private final int numAllBlocks;
  private final int numDataBlocks;
  private ExtendedBlock currentBlockGroup;
  private ExtendedBlock prevBlockGroup4Append;
  private final String[] favoredNodes;
  private final List<StripedDataStreamer> failedStreamers;
  private final Map<Integer, Integer> corruptBlockCountMap;
  private ExecutorService flushAllExecutor;
  private CompletionService<Void> flushAllExecutorCompletionService;
  private int blockGroupIndex;
  private long datanodeRestartTimeout;

  /** Construct a new output stream for creating a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes)
                         throws IOException {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating DFSStripedOutputStream for " + src);
    }

    ecPolicy = stat.getErasureCodingPolicy();
    final int numParityBlocks = ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    numDataBlocks = ecPolicy.getNumDataUnits();
    numAllBlocks = numDataBlocks + numParityBlocks; // numAllBlocks指的是一个logical block中的所有的 physical block的数量
    this.favoredNodes = favoredNodes;
    failedStreamers = new ArrayList<>();
    corruptBlockCountMap = new LinkedHashMap<>();
    flushAllExecutor = Executors.newFixedThreadPool(numAllBlocks);
    flushAllExecutorCompletionService = new
        ExecutorCompletionService<>(flushAllExecutor);

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        numDataBlocks, numParityBlocks);
    encoder = CodecUtil.createRawEncoder(dfsClient.getConfiguration(),
        ecPolicy.getCodecName(), coderOptions);

    coordinator = new Coordinator(numAllBlocks);
    cellBuffers = new CellBuffers(numParityBlocks);// 虽然初始化是用的numParityBlocks， 其实内部放的是datablock 和 parity block

    streamers = new ArrayList<>(numAllBlocks);// 并行写一个logical block中的所有internal block，但是串型写不同的logical block
    for (short i = 0; i < numAllBlocks; i++) { // 每一个physical block会有一个DataStreamer与之对应，即StrippedDataStreamer对象，用来与对应的DN进行流数据写操作
      StripedDataStreamer streamer = new StripedDataStreamer(stat,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          favoredNodes, i, coordinator, getAddBlockFlags());
      streamers.add(streamer);
    }
    currentPackets = new DFSPacket[streamers.size()];
    datanodeRestartTimeout = dfsClient.getConf().getDatanodeRestartTimeout();
    setCurrentStreamer(0); // 肯定从第一个streamer开始写
  }

  /** Construct a new output stream for appending to a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src,
      EnumSet<CreateFlag> flags, Progressable progress, LocatedBlock lastBlock,
      HdfsFileStatus stat, DataChecksum checksum, String[] favoredNodes)
      throws IOException {
    this(dfsClient, src, stat, flags, progress, checksum, favoredNodes);
    initialFileSize = stat.getLen(); // length of file when opened
    prevBlockGroup4Append = lastBlock != null ? lastBlock.getBlock() : null;
  }

  private boolean useDirectBuffer() {
    return encoder.preferDirectBuffer();
  }

  StripedDataStreamer getStripedDataStreamer(int i) {
    return streamers.get(i);
  }

  int getCurrentIndex() {
    return getCurrentStreamer().getIndex();
  }

  private synchronized StripedDataStreamer getCurrentStreamer() {
    return (StripedDataStreamer) streamer;
  }

  private synchronized StripedDataStreamer setCurrentStreamer(int newIdx) {
    // backup currentPacket for current streamer
    if (streamer != null) {
      int oldIdx = streamers.indexOf(getCurrentStreamer());
      if (oldIdx >= 0) {
        currentPackets[oldIdx] = currentPacket;
      }
    }

    streamer = getStripedDataStreamer(newIdx);
    currentPacket = currentPackets[newIdx];
    adjustChunkBoundary();

    return getCurrentStreamer();
  }

  /**
   * Encode the buffers, i.e. compute parities.
   *
   * @param buffers data buffers + parity buffers
   */
  private static void encode(RawErasureEncoder encoder, int numData,
      ByteBuffer[] buffers) throws IOException {
    final ByteBuffer[] dataBuffers = new ByteBuffer[numData]; // 这是一个ByteBuffer 的数组
    final ByteBuffer[] parityBuffers = new ByteBuffer[buffers.length - numData];
    System.arraycopy(buffers, 0, dataBuffers, 0, dataBuffers.length);
    System.arraycopy(buffers, numData, parityBuffers, 0, parityBuffers.length);

    encoder.encode(dataBuffers, parityBuffers);
  }

  /**
   * check all the existing StripedDataStreamer and find newly failed streamers.
   * @return The newly failed streamers.
   * @throws IOException if less than {@link #numDataBlocks} streamers are still
   *                     healthy.
   */
  private Set<StripedDataStreamer> checkStreamers() throws IOException {
    Set<StripedDataStreamer> newFailed = new HashSet<>();
    for(StripedDataStreamer s : streamers) {
      if (!s.isHealthy() && !failedStreamers.contains(s)) {
        newFailed.add(s);
      }
    }

    final int failCount = failedStreamers.size() + newFailed.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkStreamers: " + streamers);
      LOG.debug("healthy streamer count=" + (numAllBlocks - failCount));
      LOG.debug("original failed streamers: " + failedStreamers);
      LOG.debug("newly failed streamers: " + newFailed);
    }
    if (failCount > (numAllBlocks - numDataBlocks)) {
      closeAllStreamers();
      throw new IOException("Failed: the number of failed blocks = "
          + failCount + " > the number of parity blocks = "
          + (numAllBlocks - numDataBlocks));
    }
    return newFailed;
  }

  private void closeAllStreamers() {
    // The write has failed, Close all the streamers.
    for (StripedDataStreamer streamer : streamers) {
      streamer.close(true);
    }
  }

  private void handleCurrentStreamerFailure(String err, Exception e)
      throws IOException {
    currentPacket = null;
    handleStreamerFailure(err, e, getCurrentStreamer());
  }

  private void handleStreamerFailure(String err, Exception e,
      StripedDataStreamer streamer) throws IOException {
    LOG.warn("Failed: " + err + ", " + this, e);
    streamer.getErrorState().setInternalError();
    streamer.close(true);
    checkStreamers();
    currentPackets[streamer.getIndex()] = null;
  }

  private void replaceFailedStreamers() {
    assert streamers.size() == numAllBlocks;
    final int currentIndex = getCurrentIndex();
    assert currentIndex == 0;
    for (short i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer oldStreamer = getStripedDataStreamer(i);
      if (!oldStreamer.isHealthy()) {
        LOG.info("replacing previously failed streamer " + oldStreamer);
        StripedDataStreamer streamer = new StripedDataStreamer(oldStreamer.stat,
            dfsClient, src, oldStreamer.progress,
            oldStreamer.checksum4WriteBlock, cachingStrategy, byteArrayManager,
            favoredNodes, i, coordinator, getAddBlockFlags());
        streamers.set(i, streamer);
        currentPackets[i] = null;
        if (i == currentIndex) {
          this.streamer = streamer;
          this.currentPacket = null;
        }
        streamer.start();
      }
    }
  }

  private void waitEndBlocks(int i) throws IOException {
    while (getStripedDataStreamer(i).isHealthy()) {
      final ExtendedBlock b = coordinator.endBlocks.takeWithTimeout(i);
      if (b != null) {
        StripedBlockUtil.checkBlocks(currentBlockGroup, i, b);
        return;
      }
    }
  }

  private DatanodeInfo[] getExcludedNodes() {
    List<DatanodeInfo> excluded = new ArrayList<>();
    for (StripedDataStreamer streamer : streamers) {
      for (DatanodeInfo e : streamer.getExcludedNodes()) {
        if (e != null) {
          excluded.add(e);
        }
      }
    }
    return excluded.toArray(new DatanodeInfo[excluded.size()]);
  }

  /**
   * 在Stripe存里面，block的意思其实是block group，这里是为一个Block group分配datanode
   * @throws IOException
   */
  private void allocateNewBlock() throws IOException {
    if (currentBlockGroup != null) {
      for (int i = 0; i < numAllBlocks; i++) {
        // sync all the healthy streamers before writing to the new block
        waitEndBlocks(i);
      }
    }
    failedStreamers.clear();
    DatanodeInfo[] excludedNodes = getExcludedNodes();
    LOG.debug("Excluding DataNodes when allocating new block: "
        + Arrays.asList(excludedNodes));

    // replace failed streamers
    ExtendedBlock prevBlockGroup = currentBlockGroup; // stripe中的block group对应到了传统的block
    if (prevBlockGroup4Append != null) {
      prevBlockGroup = prevBlockGroup4Append;
      prevBlockGroup4Append = null;
    }
    replaceFailedStreamers();

    LOG.debug("Allocating new block group. The previous block group: "
        + prevBlockGroup);
    final LocatedBlock lb;
    try {
      // 向NameNode申请Block.对于Stripped的场景，是申请一个BlockGroup(Logic block)
      // 这个addBlock是调用的super class的addBlock(), 即DFSStrippedOutputStream和DFSOutputStream都是一样的addBlock()逻辑
      // 从代码来看，这里并没有blocksize的信息
      lb = addBlock(excludedNodes, dfsClient, src,
          prevBlockGroup, fileId, favoredNodes, getAddBlockFlags());
    } catch (IOException ioe) {
      closeAllStreamers();
      throw ioe;
    }
    assert lb.isStriped();
    // assign the new block to the current block group
    currentBlockGroup = lb.getBlock();
    blockGroupIndex++;
    // 从申请到的LocatedBlock(对应了一个Block Group)切分出Internal block
    // 这里的blocks的索引已经是跟internal block在group中的索引一致了，每一个internal block的大小也从Block group中拆分了
    final LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(
        (LocatedStripedBlock) lb, cellSize, numDataBlocks,
        numAllBlocks - numDataBlocks);
    for (int i = 0; i < blocks.length; i++) {
      StripedDataStreamer si = getStripedDataStreamer(i);
      assert si.isHealthy();
      if (blocks[i] == null) {
        // allocBlock() should guarantee that all data blocks are successfully
        // allocated.
        assert i >= numDataBlocks; // 不可能有data block没有分配上
        // Set exception and close streamer as there is no block locations
        // found for the parity block.
        LOG.warn("Cannot allocate parity block(index={}, policy={}). " +
                "Exclude nodes={}. There may not be enough datanodes or " +
                "racks. You can check if the cluster topology supports " +
                "the enabled erasure coding policies by running the command " +
                "'hdfs ec -verifyClusterSetup'.", i,  ecPolicy.getName(),
            excludedNodes);
        si.getLastException().set(
            new IOException("Failed to get parity block, index=" + i));
        si.getErrorState().setInternalError();
        si.close(true);
      } else {
        // 把这个Block group里面的所有的internal block分配给每一个streamer
        coordinator.getFollowingBlocks().offer(i, blocks[i]);
      }
    }
  }

  /**
   * 这个blockSize就是创建文件的时候配置文件中的block size， 128MB
   * 这个block group写文件的过程中currentBlockGroup.getNumBytes()不断更新，如果发现urrentBlockGroup.getNumBytes() == blockSize * numDataBlocks， 说明这个group写完了，可以写
   * 下一个group了
   * @return
   */
  private boolean shouldEndBlockGroup() {
    return currentBlockGroup != null && // 这个blockSize肯定是一个internal block的size，到底是128MB还是 128MB/6?
        currentBlockGroup.getNumBytes() == blockSize * numDataBlocks;
  }

  /**
   * chunk是计算checksum的单位，以chunk为单位计算checksum，以packet为单位发送数据
   * 将bytes中[offset, offset+len]的数据作为一个chunk写入packet(从调用者的代码可以看到，这个数据只可能小于等于一个chunk的长度)
   * 通过currentPacket和currentStream来确定是由哪个streamer来负责写这个数据
   *
   * @throws IOException
   */
  @Override
  protected synchronized void writeChunk(byte[] bytes, int offset, int len,
      byte[] checksum, int ckoff, int cklen) throws IOException {
    final int index = getCurrentIndex();
    final int pos = cellBuffers.addTo(index, bytes, offset, len); // 往data block中写入数据，返回更新以后的position
    final boolean cellFull = pos == cellSize; // 根据不同的Stripe Policy确定的，默认是1MB

    //  如果是第一个block，或者当前的block group写完了，需要写到一个新的block group，那么就创建一个新的
    if (currentBlockGroup == null || shouldEndBlockGroup()) {
      // the incoming data should belong to a new block. Allocate a new block.
      allocateNewBlock(); // 对于一个刚刚创建的currentBlockGroup, numBytes=0
    }

    currentBlockGroup.setNumBytes(currentBlockGroup.getNumBytes() + len);// 在写入过程中，numbers逐渐增加
    // note: the current streamer can be refreshed after allocating a new block
    final StripedDataStreamer current = getCurrentStreamer();
    if (current.isHealthy()) {
      try {
        // 将当前的chunk写入到Packet中（不代表packet需要enqueue了。）
        super.writeChunk(bytes, offset, len, checksum, ckoff, cklen);
      } catch(Exception e) {
        handleCurrentStreamerFailure("offset=" + offset + ", length=" + len, e);
      }
    }

    // Two extra steps are needed when a striping cell is full:
    // 1. Forward the current index pointer
    // 2. Generate parity packets if a full stripe of data cells are present
<<<<<<< HEAD
    if (cellFull) { // cell写满了，需要将stream切到下一个
      int next = index + 1;
=======
    if (cellFull) {
      int next = index + 1; // 写完了一个cell，切换到下一个cell
>>>>>>> d7581f89471 (dfs)
      //When all data cells in a stripe are ready, we need to encode
      //them and generate some parity cells. These cells will be
      //converted to packets and put to their DataStreamer's queue.
      // 如果不是最后一个data block, 是不需要写parity chunk的
      if (next == numDataBlocks) { // 刚刚写的是这个logic group中的最后一个data block的chunk，意味着下一个chunk是写parity block的chunk
        cellBuffers.flipDataBuffers();
<<<<<<< HEAD
        writeParityCells(); // 在写parity的过程中，stream也是在往后切换的
        next = 0;
=======
        writeParityCells();
        next = 0; // 写完了所有的parity，index就从0开始重新写入
>>>>>>> d7581f89471 (dfs)

        // if this is the end of the block group, end each internal block
        if (shouldEndBlockGroup()) {
          flushAllInternals();
          checkStreamerFailures(false);
          for (int i = 0; i < numAllBlocks; i++) {
            final StripedDataStreamer s = setCurrentStreamer(i);
            if (s.isHealthy()) {
              try {
                endBlock();
              } catch (IOException ignored) {}
            }
          }
        } else {
          // check failure state for all the streamers. Bump GS if necessary
          checkStreamerFailures(true);
        }
      }
      setCurrentStreamer(next);// 将当前stream切换到下一个，因为当前cell写满了。不写满不切换stream
      /**
       * 写下一个cell。其中，
       * 如果 next != numDataBlocks, 那么这里的下一个cell就是当前stripe的下一个cell
       * 如果next == numDataBlocks, 那么这里的下一个cell就是下一个stripe的第一个cell
       */
      setCurrentStreamer(next); //
    }
  }

  @Override
  synchronized void enqueueCurrentPacketFull() throws IOException {
    LOG.debug("enqueue full {}, src={}, bytesCurBlock={}, blockSize={},"
            + " appendChunk={}, {}", currentPacket, src, getStreamer()
            .getBytesCurBlock(), blockSize, getStreamer().getAppendChunk(),
        getStreamer());
    enqueueCurrentPacket();
    adjustChunkBoundary();
    // no need to end block here
  }

  /**
   * @return whether the data streamer with the given index is streaming data.
   * Note the streamer may not be in STREAMING stage if the block length is less
   * than a stripe.
   */
  private boolean isStreamerWriting(int streamerIndex) {
    final long length = currentBlockGroup == null ?
        0 : currentBlockGroup.getNumBytes();
    if (length == 0) {
      return false;
    }
    if (streamerIndex >= numDataBlocks) {
      return true;
    }
    final int numCells = (int) ((length - 1) / cellSize + 1);
    return streamerIndex < numCells;
  }

  private Set<StripedDataStreamer> markExternalErrorOnStreamers() {
    Set<StripedDataStreamer> healthySet = new HashSet<>();
    for (int i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer streamer = getStripedDataStreamer(i);
      if (streamer.isHealthy() && isStreamerWriting(i)) {
        Preconditions.checkState(
            streamer.getStage() == BlockConstructionStage.DATA_STREAMING,
            "streamer: " + streamer);
        streamer.setExternalError();
        healthySet.add(streamer);
      } else if (!streamer.streamerClosed()
          && streamer.getErrorState().hasDatanodeError()
          && streamer.getErrorState().doWaitForRestart()) {
        healthySet.add(streamer);
        failedStreamers.remove(streamer);
      }
    }
    return healthySet;
  }

  /**
   * Check and handle data streamer failures. This is called only when we have
   * written a full stripe (i.e., enqueue all packets for a full stripe), or
   * when we're closing the outputstream.
   */
  private void checkStreamerFailures(boolean isNeedFlushAllPackets)
      throws IOException {
    Set<StripedDataStreamer> newFailed = checkStreamers();
    if (newFailed.size() == 0) {
      return;
    }

    if (isNeedFlushAllPackets) {
      // for healthy streamers, wait till all of them have fetched the new block
      // and flushed out all the enqueued packets.
      flushAllInternals();
    }
    // recheck failed streamers again after the flush
    newFailed = checkStreamers();
    while (newFailed.size() > 0) {
      failedStreamers.addAll(newFailed);
      coordinator.clearFailureStates();
      corruptBlockCountMap.put(blockGroupIndex, failedStreamers.size());

      // mark all the healthy streamers as external error
      Set<StripedDataStreamer> healthySet = markExternalErrorOnStreamers();

      // we have newly failed streamers, update block for pipeline
      final ExtendedBlock newBG = updateBlockForPipeline(healthySet);

      // wait till all the healthy streamers to
      // 1) get the updated block info
      // 2) create new block outputstream
      newFailed = waitCreatingStreamers(healthySet);
      if (newFailed.size() + failedStreamers.size() >
          numAllBlocks - numDataBlocks) {
        // The write has failed, Close all the streamers.
        closeAllStreamers();
        throw new IOException(
            "Data streamers failed while creating new block streams: "
                + newFailed + ". There are not enough healthy streamers.");
      }
      for (StripedDataStreamer failedStreamer : newFailed) {
        assert !failedStreamer.isHealthy();
      }

      // TODO we can also succeed if all the failed streamers have not taken
      // the updated block
      if (newFailed.size() == 0) {
        // reset external error state of all the streamers
        for (StripedDataStreamer streamer : healthySet) {
          assert streamer.isHealthy();
          streamer.getErrorState().reset();
        }
        updatePipeline(newBG);
      }
      for (int i = 0; i < numAllBlocks; i++) {
        coordinator.offerStreamerUpdateResult(i, newFailed.size() == 0);
      }
      //wait for get notify to failed stream
      if (newFailed.size() != 0) {
        try {
          Thread.sleep(datanodeRestartTimeout);
        } catch (InterruptedException e) {
          // Do nothing
        }
      }
    }
  }

  /**
   * Check if the streamers were successfully updated, adding failed streamers
   * in the <i>failed</i> return parameter.
   * @param failed Return parameter containing failed streamers from
   *               <i>streamers</i>.
   * @param streamers Set of streamers that are being updated
   * @return total number of successful updates and failures
   */
  private int checkStreamerUpdates(Set<StripedDataStreamer> failed,
      Set<StripedDataStreamer> streamers) {
    for (StripedDataStreamer streamer : streamers) {
      if (!coordinator.updateStreamerMap.containsKey(streamer)) {
        if (!streamer.isHealthy() &&
            coordinator.getNewBlocks().peek(streamer.getIndex()) != null) {
          // this streamer had internal error before getting updated block
          failed.add(streamer);
        }
      }
    }
    return coordinator.updateStreamerMap.size() + failed.size();
  }

  /**
   * Waits for streamers to be created.
   *
   * @param healthyStreamers Set of healthy streamers
   * @return Set of streamers that failed.
   *
   * @throws IOException
   */
  private Set<StripedDataStreamer> waitCreatingStreamers(
      Set<StripedDataStreamer> healthyStreamers) throws IOException {
    Set<StripedDataStreamer> failed = new HashSet<>();
    final int expectedNum = healthyStreamers.size();
    final long socketTimeout = dfsClient.getConf().getSocketTimeout();
    // the total wait time should be less than the socket timeout, otherwise
    // a slow streamer may cause other streamers to timeout. here we wait for
    // half of the socket timeout
    long remaingTime = socketTimeout > 0 ? socketTimeout/2 : Long.MAX_VALUE;
    final long waitInterval = 1000;
    synchronized (coordinator) {
      while (checkStreamerUpdates(failed, healthyStreamers) < expectedNum
          && remaingTime > 0) {
        try {
          long start = Time.monotonicNow();
          coordinator.wait(waitInterval);
          remaingTime -= Time.monotonicNow() - start;
        } catch (InterruptedException e) {
          throw DFSUtilClient.toInterruptedIOException("Interrupted when waiting" +
              " for results of updating striped streamers", e);
        }
      }
    }
    synchronized (coordinator) {
      for (StripedDataStreamer streamer : healthyStreamers) {
        if (!coordinator.updateStreamerMap.containsKey(streamer)) {
          // close the streamer if it is too slow to create new connection
          LOG.info("close the slow stream " + streamer);
          streamer.setStreamerAsClosed();
          failed.add(streamer);
        }
      }
    }
    for (Map.Entry<StripedDataStreamer, Boolean> entry :
        coordinator.updateStreamerMap.entrySet()) {
      if (!entry.getValue()) {
        failed.add(entry.getKey());
      }
    }
    for (StripedDataStreamer failedStreamer : failed) {
      healthyStreamers.remove(failedStreamer);
    }
    return failed;
  }

  /**
   * Call {@link ClientProtocol#updateBlockForPipeline} and assign updated block
   * to healthy streamers.
   * @param healthyStreamers The healthy data streamers. These streamers join
   *                         the failure handling.
   */
  private ExtendedBlock updateBlockForPipeline(
      Set<StripedDataStreamer> healthyStreamers) throws IOException {
    final LocatedBlock updated = dfsClient.namenode.updateBlockForPipeline(
        currentBlockGroup, dfsClient.clientName);
    final long newGS = updated.getBlock().getGenerationStamp();
    ExtendedBlock newBlock = new ExtendedBlock(currentBlockGroup);
    newBlock.setGenerationStamp(newGS);
    final LocatedBlock[] updatedBlks = StripedBlockUtil.parseStripedBlockGroup(
        (LocatedStripedBlock) updated, cellSize, numDataBlocks,
        numAllBlocks - numDataBlocks);

    for (int i = 0; i < numAllBlocks; i++) {
      StripedDataStreamer si = getStripedDataStreamer(i);
      if (healthyStreamers.contains(si)) {
        final LocatedBlock lb = new LocatedBlock(new ExtendedBlock(newBlock),
            null, null, null, -1, updated.isCorrupt(), null);
        lb.setBlockToken(updatedBlks[i].getBlockToken());
        coordinator.getNewBlocks().offer(i, lb);
      }
    }
    return newBlock;
  }

  private void updatePipeline(ExtendedBlock newBG) throws IOException {
    final DatanodeInfo[] newNodes = new DatanodeInfo[numAllBlocks];
    final String[] newStorageIDs = new String[numAllBlocks];
    for (int i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer streamer = getStripedDataStreamer(i);
      final DatanodeInfo[] nodes = streamer.getNodes();
      final String[] storageIDs = streamer.getStorageIDs();
      if (streamer.isHealthy() && nodes != null && storageIDs != null) {
        newNodes[i] = nodes[0];
        newStorageIDs[i] = storageIDs[0];
      } else {
        newNodes[i] = new DatanodeInfoBuilder()
            .setNodeID(DatanodeID.EMPTY_DATANODE_ID).build();
        newStorageIDs[i] = "";
      }
    }

    // Update the NameNode with the acked length of the block group
    // Save and restore the unacked length
    final long sentBytes = currentBlockGroup.getNumBytes();
    final long ackedBytes = getAckedLength();
    Preconditions.checkState(ackedBytes <= sentBytes,
        "Acked:" + ackedBytes + ", Sent:" + sentBytes);
    currentBlockGroup.setNumBytes(ackedBytes);
    newBG.setNumBytes(ackedBytes);
    dfsClient.namenode.updatePipeline(dfsClient.clientName, currentBlockGroup,
        newBG, newNodes, newStorageIDs);
    currentBlockGroup = newBG;
    currentBlockGroup.setNumBytes(sentBytes);
  }

  /**
   * Return the length of each block in the block group.
   * Unhealthy blocks have a length of -1.
   *
   * @return List of block lengths.
   */
  private List<Long> getBlockLengths() {
    List<Long> blockLengths = new ArrayList<>(numAllBlocks);
    for (int i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer streamer = getStripedDataStreamer(i);
      long numBytes = -1;
      if (streamer.isHealthy()) {
        if (streamer.getBlock() != null) {
          numBytes = streamer.getBlock().getNumBytes();
        }
      }
      blockLengths.add(numBytes);
    }
    return blockLengths;
  }

  /**
   * Get the length of acked bytes in the block group.
   *
   * <p>
   *   A full stripe is acked when at least numDataBlocks streamers have
   *   the corresponding cells of the stripe, and all previous full stripes are
   *   also acked. This enforces the constraint that there is at most one
   *   partial stripe.
   * </p>
   * <p>
   *   Partial stripes write all parity cells. Empty data cells are not written.
   *   Parity cells are the length of the longest data cell(s). For example,
   *   with RS(3,2), if we have data cells with lengths [1MB, 64KB, 0], the
   *   parity blocks will be length [1MB, 1MB].
   * </p>
   * <p>
   *   To be considered acked, a partial stripe needs at least numDataBlocks
   *   empty or written cells.
   * </p>
   * <p>
   *   Currently, partial stripes can only happen when closing the file at a
   *   non-stripe boundary, but this could also happen during (currently
   *   unimplemented) hflush/hsync support.
   * </p>
   */
  private long getAckedLength() {
    // Determine the number of full stripes that are sufficiently durable
    final long sentBytes = currentBlockGroup.getNumBytes();
    final long numFullStripes = sentBytes / numDataBlocks / cellSize;
    final long fullStripeLength = numFullStripes * numDataBlocks * cellSize;
    assert fullStripeLength <= sentBytes : "Full stripe length can't be " +
        "greater than the block group length";

    long ackedLength = 0;

    // Determine the length contained by at least `numDataBlocks` blocks.
    // Since it's sorted, all the blocks after `offset` are at least as long,
    // and there are at least `numDataBlocks` at or after `offset`.
    List<Long> blockLengths = Collections.unmodifiableList(getBlockLengths());
    List<Long> sortedBlockLengths = new ArrayList<>(blockLengths);
    Collections.sort(sortedBlockLengths);
    if (numFullStripes > 0) {
      final int offset = sortedBlockLengths.size() - numDataBlocks;
      ackedLength = sortedBlockLengths.get(offset) * numDataBlocks;
    }

    // If the acked length is less than the expected full stripe length, then
    // we're missing a full stripe. Return the acked length.
    if (ackedLength < fullStripeLength) {
      return ackedLength;
    }
    // If the expected length is exactly a stripe boundary, then we're also done
    if (ackedLength == sentBytes) {
      return ackedLength;
    }

    /*
    Otherwise, we're potentially dealing with a partial stripe.
    The partial stripe is laid out as follows:

      0 or more full data cells, `cellSize` in length.
      0 or 1 partial data cells.
      0 or more empty data cells.
      `numParityBlocks` parity cells, the length of the longest data cell.

    If the partial stripe is sufficiently acked, we'll update the ackedLength.
    */

    // How many full and empty data cells do we expect?
    final int numFullDataCells = (int)
        ((sentBytes - fullStripeLength) / cellSize);
    final int partialLength = (int) (sentBytes - fullStripeLength) % cellSize;
    final int numPartialDataCells = partialLength == 0 ? 0 : 1;
    final int numEmptyDataCells = numDataBlocks - numFullDataCells -
        numPartialDataCells;
    // Calculate the expected length of the parity blocks.
    final int parityLength = numFullDataCells > 0 ? cellSize : partialLength;

    final long fullStripeBlockOffset = fullStripeLength / numDataBlocks;

    // Iterate through each type of streamers, checking the expected length.
    long[] expectedBlockLengths = new long[numAllBlocks];
    int idx = 0;
    // Full cells
    for (; idx < numFullDataCells; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + cellSize;
    }
    // Partial cell
    for (; idx < numFullDataCells + numPartialDataCells; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + partialLength;
    }
    // Empty cells
    for (; idx < numFullDataCells + numPartialDataCells + numEmptyDataCells;
         idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset;
    }
    // Parity cells
    for (; idx < numAllBlocks; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + parityLength;
    }

    // Check expected lengths against actual streamer lengths.
    // Update if we have sufficient durability.
    int numBlocksWithCorrectLength = 0;
    for (int i = 0; i < numAllBlocks; i++) {
      if (blockLengths.get(i) == expectedBlockLengths[i]) {
        numBlocksWithCorrectLength++;
      }
    }
    if (numBlocksWithCorrectLength >= numDataBlocks) {
      ackedLength = sentBytes;
    }

    return ackedLength;
  }

  private int stripeDataSize() {
    return numDataBlocks * cellSize;
  }

  @Override
  public boolean hasCapability(String capability) {
    // StreamCapabilities like hsync / hflush are not supported yet.
    return false;
  }

  @Override
  public void hflush() {
    // not supported yet
    LOG.debug("DFSStripedOutputStream does not support hflush. "
        + "Caller should check StreamCapabilities before calling.");
  }

  @Override
  public void hsync() {
    // not supported yet
    LOG.debug("DFSStripedOutputStream does not support hsync. "
        + "Caller should check StreamCapabilities before calling.");
  }

  @Override
  public void hsync(EnumSet<SyncFlag> syncFlags) {
    // not supported yet
    LOG.debug("DFSStripedOutputStream does not support hsync {}. "
        + "Caller should check StreamCapabilities before calling.", syncFlags);
  }

  @Override
  protected synchronized void start() {
    for (StripedDataStreamer streamer : streamers) {
      streamer.start();
    }
  }

  @Override
  void abort() throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    synchronized (this) {
      if (isClosed()) {
        return;
      }
      exceptionLastSeen.set(new IOException("Lease timeout of "
          + (dfsClient.getConf().getHdfsTimeout() / 1000)
          + " seconds expired."));

      try {
        closeThreads(true);
      } catch (IOException e) {
        b.add(e);
      }
    }

    dfsClient.endFileLease(fileId);
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  @Override
  boolean isClosed() {
    if (closed) {
      return true;
    }
    for(StripedDataStreamer s : streamers) {
      if (!s.streamerClosed()) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void closeThreads(boolean force) throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    try {
      for (StripedDataStreamer streamer : streamers) {
        try {
          streamer.close(force);
          streamer.join();
          streamer.closeSocket();
        } catch (Exception e) {
          try {
            handleStreamerFailure("force=" + force, e, streamer);
          } catch (IOException ioe) {
            b.add(ioe);
          }
        } finally {
          streamer.setSocketToNull();
        }
      }
    } finally {
      setClosed();
    }
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  private boolean generateParityCellsForLastStripe() {
    final long currentBlockGroupBytes = currentBlockGroup == null ?
        0 : currentBlockGroup.getNumBytes();
    final long lastStripeSize = currentBlockGroupBytes % stripeDataSize();
    if (lastStripeSize == 0) {
      return false;
    }

    final long parityCellSize = lastStripeSize < cellSize?
        lastStripeSize : cellSize;
    final ByteBuffer[] buffers = cellBuffers.getBuffers();

    for (int i = 0; i < numAllBlocks; i++) {
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      final int position = buffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller" +
          " than parity block, then its last cell should be small than last" +
          " parity cell";
      for (int j = 0; j < parityCellSize - position; j++) {
        buffers[i].put((byte) 0);
      }
      buffers[i].flip();
    }
    return true;
  }

  void writeParityCells() throws IOException {
    final ByteBuffer[] buffers = cellBuffers.getBuffers();
    // Skips encoding and writing parity cells if there are no healthy parity
    // data streamers
    if (!checkAnyParityStreamerIsHealthy()) {
      return;
    }
    //encode the data cells
    encode(encoder, numDataBlocks, buffers);
    for (int i = numDataBlocks; i < numAllBlocks; i++) { // 写当前的logic block中的一个stripe的所有parity block
      writeParity(i, buffers[i], cellBuffers.getChecksumArray(i));
    }
    cellBuffers.clear();
  }

  private boolean checkAnyParityStreamerIsHealthy() {
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
      if (streamers.get(i).isHealthy()) { // 从这里看到，写parity的streamer是独立的streamer
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Skips encoding and writing parity cells as there are "
          + "no healthy parity data streamers: " + streamers);
    }
    return false;
  }

  /**
   * 这个i代表的是在group中的internal block的索引，因此在写parity的时候，index从numDataBlocks开始
   * 给parity计算checksum，并写入到packet中去
   * @param index
   * @param buffer
   * @param checksumBuf
   * @throws IOException
   */
  void writeParity(int index, ByteBuffer buffer, byte[] checksumBuf)
      throws IOException {
    final StripedDataStreamer current = setCurrentStreamer(index); // 获取到写当前这个internal parity block的streamer
    final int len = buffer.limit();

    final long oldBytes = current.getBytesCurBlock();
    if (current.isHealthy()) {
      try {
        DataChecksum sum = getDataChecksum();
        if (buffer.isDirect()) {
          ByteBuffer directCheckSumBuf =
              BUFFER_POOL.getBuffer(true, checksumBuf.length);
          sum.calculateChunkedSums(buffer, directCheckSumBuf);// 计算checksum，放在directCheckSumBuf
          directCheckSumBuf.get(checksumBuf);// 把数据从directCheckSumBuf拷贝到 checksumBuf
          BUFFER_POOL.putBuffer(directCheckSumBuf);// 释放directCheckSumBuf到pool中去
        } else {
          sum.calculateChunkedSums(buffer.array(), 0, len, checksumBuf, 0);
        }

        for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
          int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
          int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize(); // 确定在checksum数组中的位置
          super.writeChunk(buffer, chunkLen, checksumBuf, ckOffset, // 写入parity. 此时parity跟普通的internal block是一样的，没有差别
              getChecksumSize()); // 将chunk伴随checksum写入到当前的packet中，所以，data chunk和parity chunk是无差别地写入到相同的packet中去的
        }
      } catch(Exception e) {
        handleCurrentStreamerFailure("oldBytes=" + oldBytes + ", len=" + len,
            e);
      }
    }
  }

  @Override
  void setClosed() {
    super.setClosed();
    for (int i = 0; i < numAllBlocks; i++) {
      getStripedDataStreamer(i).release();
    }
    cellBuffers.release();
  }

  @Override
  protected synchronized void closeImpl() throws IOException {
    try {
      if (isClosed()) {
        exceptionLastSeen.check(true);

        // Writing to at least {dataUnits} replicas can be considered as
        //  success, and the rest of data can be recovered.
        final int minReplication = ecPolicy.getNumDataUnits();
        int goodStreamers = 0;
        final MultipleIOException.Builder b = new MultipleIOException.Builder();
        for (final StripedDataStreamer si : streamers) {
          try {
            si.getLastException().check(true);
            goodStreamers++;
          } catch (IOException e) {
            b.add(e);
          }
        }
        if (goodStreamers < minReplication) {
          final IOException ioe = b.build();
          if (ioe != null) {
            throw ioe;
          }
        }
        return;
      }

      try {
        // flush from all upper layers
        flushBuffer();
        // if the last stripe is incomplete, generate and write parity cells
        if (generateParityCellsForLastStripe()) {
          writeParityCells();
        }
        enqueueAllCurrentPackets();

        // flush all the data packets
        flushAllInternals();
        // check failures
        checkStreamerFailures(false);

        for (int i = 0; i < numAllBlocks; i++) {
          final StripedDataStreamer s = setCurrentStreamer(i);
          if (s.isHealthy()) {
            try {
              if (s.getBytesCurBlock() > 0) {
                setCurrentPacketToEmpty();
              }
              // flush the last "close" packet to Datanode
              flushInternal();
            } catch (Exception e) {
              // TODO for both close and endBlock, we currently do not handle
              // failures when sending the last packet. We actually do not need to
              // bump GS for this kind of failure. Thus counting the total number
              // of failures may be good enough.
            }
          }
        }
      } finally {
        // Failures may happen when flushing data/parity data out. Exceptions
        // may be thrown if the number of failed streamers is more than the
        // number of parity blocks, or updatePipeline RPC fails. Streamers may
        // keep waiting for the new block/GS information. Thus need to force
        // closing these threads.
        closeThreads(true);
      }

      try (TraceScope ignored =
               dfsClient.getTracer().newScope("completeFile")) {
        completeFile(currentBlockGroup);
      }
      logCorruptBlocks();
    } catch (ClosedChannelException ignored) {
    } finally {
      setClosed();
      // shutdown executor of flushAll tasks
      flushAllExecutor.shutdownNow();
      encoder.release();
    }
  }

  @VisibleForTesting
  void enqueueAllCurrentPackets() throws IOException {
    int idx = streamers.indexOf(getCurrentStreamer());
    for(int i = 0; i < streamers.size(); i++) {
      final StripedDataStreamer si = setCurrentStreamer(i);
      if (si.isHealthy() && currentPacket != null) {
        try {
          enqueueCurrentPacket();
        } catch (IOException e) {
          handleCurrentStreamerFailure("enqueueAllCurrentPackets, i=" + i, e);
        }
      }
    }
    setCurrentStreamer(idx);
  }

  void flushAllInternals() throws IOException {
    Map<Future<Void>, Integer> flushAllFuturesMap = new HashMap<>();
    Future<Void> future = null;
    int current = getCurrentIndex();

    for (int i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer s = setCurrentStreamer(i);
      if (s.isHealthy()) {
        try {
          // flush all data to Datanode
          final long toWaitFor = flushInternalWithoutWaitingAck();
          future = flushAllExecutorCompletionService.submit(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  s.waitForAckedSeqno(toWaitFor);
                  return null;
                }
              });
          flushAllFuturesMap.put(future, i);
        } catch (Exception e) {
          handleCurrentStreamerFailure("flushInternal " + s, e);
        }
      }
    }
    setCurrentStreamer(current);
    for (int i = 0; i < flushAllFuturesMap.size(); i++) {
      try {
        future = flushAllExecutorCompletionService.take();
        future.get();
      } catch (InterruptedException ie) {
        throw DFSUtilClient.toInterruptedIOException(
            "Interrupted during waiting all streamer flush, ", ie);
      } catch (ExecutionException ee) {
        LOG.warn(
            "Caught ExecutionException while waiting all streamer flush, ", ee);
        StripedDataStreamer s = streamers.get(flushAllFuturesMap.get(future));
        handleStreamerFailure("flushInternal " + s,
            (Exception) ee.getCause(), s);
      }
    }
  }

  static void sleep(long ms, String op) throws InterruptedIOException {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException ie) {
      throw DFSUtilClient.toInterruptedIOException(
          "Sleep interrupted during " + op, ie);
    }
  }

  private void logCorruptBlocks() {
    for (Map.Entry<Integer, Integer> entry : corruptBlockCountMap.entrySet()) {
      int bgIndex = entry.getKey();
      int corruptBlockCount = entry.getValue();
      StringBuilder sb = new StringBuilder();
      sb.append("Block group <").append(bgIndex).append("> failed to write ")
          .append(corruptBlockCount).append(" blocks.");
      if (corruptBlockCount == numAllBlocks - numDataBlocks) {
        sb.append(" It's at high risk of losing data.");
      }
      LOG.warn(sb.toString());
    }
  }

  @Override
  ExtendedBlock getBlock() {
    return currentBlockGroup;
  }
}
