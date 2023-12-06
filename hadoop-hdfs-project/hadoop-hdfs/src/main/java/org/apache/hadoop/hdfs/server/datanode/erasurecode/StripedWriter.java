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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Manage striped writers that writes to a target with reconstructed data.
 * 有多个writer，可以看到一个StripedWriter负责一个BlockGroup 的一个或者多个internal block的恢复，下面绑定了多个StripedBlockWriter
 */
@InterfaceAudience.Private
class StripedWriter {
  private static final Logger LOG = DataNode.LOG;
  private final static int WRITE_PACKET_SIZE = 64 * 1024;

  private final StripedReconstructor reconstructor;
  private final DataNode datanode;
  private final Configuration conf;

  private final int dataBlkNum;
  private final int parityBlkNum;

  private boolean[] targetsStatus;

  // targets
  private final DatanodeInfo[] targets;
  private final short[] targetIndices;
  private boolean hasValidTargets;
  private final StorageType[] targetStorageTypes;
  private final String[] targetStorageIds;

  private StripedBlockWriter[] writers;

  private int maxChunksPerPacket;
  private byte[] packetBuf;
  private byte[] checksumBuf;
  private int bytesPerChecksum;
  private int checksumSize;

  /**
   * 负责一个Block Group的恢复工作
   * @param reconstructor
   * @param datanode
   * @param conf
   * @param stripedReconInfo
   */
  StripedWriter(StripedReconstructor reconstructor, DataNode datanode,
      Configuration conf, StripedReconstructionInfo stripedReconInfo) {
    this.reconstructor = reconstructor;
    this.datanode = datanode;
    this.conf = conf;

    dataBlkNum = stripedReconInfo.getEcPolicy().getNumDataUnits();
    parityBlkNum = stripedReconInfo.getEcPolicy().getNumParityUnits();

    this.targets = stripedReconInfo.getTargets();
    assert targets != null;
    this.targetStorageTypes = stripedReconInfo.getTargetStorageTypes();
    assert targetStorageTypes != null;
    this.targetStorageIds = stripedReconInfo.getTargetStorageIds();
    assert targetStorageIds != null;

    /**
     * 有多个writer，可以看到一个StripedWriter负责一个BlockGroup 的一个或者多个internal block的恢复
     * 一个StripedBlockWriter负责一个 replica的生成操作
     */
    writers = new StripedBlockWriter[targets.length];

    targetIndices = new short[targets.length];//targetIndices数组中的每一个元素的值代表了这个target在Block Index中的索引
    Preconditions.checkArgument(targetIndices.length <= parityBlkNum,
        "Too much missed striped blocks.");
    initTargetIndices();
    long maxTargetLength = 0L;
    for (short targetIndex : targetIndices) { // 遍历每一个internal block，获取最大的internal block的长度
      maxTargetLength = Math.max(maxTargetLength,
          reconstructor.getBlockLen(targetIndex));
    }
    reconstructor.setMaxTargetLength(maxTargetLength);

    // targetsStatus store whether some target is success, it will record
    // any failed target once, if some target failed (invalid DN or transfer
    // failed), will not transfer data to it any more.
    targetsStatus = new boolean[targets.length];
  }

  void init() throws IOException {
    DataChecksum checksum = reconstructor.getChecksum();
    checksumSize = checksum.getChecksumSize();// 4B
    bytesPerChecksum = checksum.getBytesPerChecksum(); // 512B
    int chunkSize = bytesPerChecksum + checksumSize; // 516B
    maxChunksPerPacket = Math.max( // WRITE_PACKET_SIZE是64KB,需要包含PKT_MAX_HEADER_LEN的header
        (WRITE_PACKET_SIZE - PacketHeader.PKT_MAX_HEADER_LEN) / chunkSize, 1);
    int maxPacketSize = chunkSize * maxChunksPerPacket
        + PacketHeader.PKT_MAX_HEADER_LEN; // 最大的packet的大小

    packetBuf = new byte[maxPacketSize];
    int tmpLen = checksumSize *
        (reconstructor.getBufferSize() / bytesPerChecksum);
    checksumBuf = new byte[tmpLen]; // checksum的buffer的长度

    if (initTargetStreams() == 0) {
      String error = "All targets are failed.";
      throw new IOException(error);
    }
  }

  private void initTargetIndices() {
    BitSet bitset = reconstructor.getLiveBitSet();

    int m = 0;
    hasValidTargets = false;
    for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
      if (!bitset.get(i)) { // 缺数据
        if (reconstructor.getBlockLen(i) > 0) { // 的确需要数据
          if (m < targets.length) { // 最多构造m个target
            targetIndices[m++] = (short)i;
            hasValidTargets = true;
          }
        }
      }
    }
  }

  /**
   * Send reconstructed data to targets.
   */
  int transferData2Targets() {
    int nSuccess = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        boolean success = false;
        try {
          writers[i].transferData2Target(packetBuf);
          nSuccess++;
          success = true;
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
        targetsStatus[i] = success;
      }
    }
    return nSuccess;
  }

  /**
   * Send an empty packet to mark the end of the block.
   */
  void endTargetBlocks() {
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        try {
          writers[i].endTargetBlock(packetBuf);
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
      }
    }
  }

  /**
   * Initialize  output/input streams for transferring data to target
   * and send create block request.
   */
  int initTargetStreams() {
    int nSuccess = 0;
    for (short i = 0; i < targets.length; i++) {
      try {
        writers[i] = createWriter(i);
        nSuccess++;
        targetsStatus[i] = true;
      } catch (Throwable e) {
        LOG.warn(e.getMessage());
      }
    }
    return nSuccess;
  }

  private StripedBlockWriter createWriter(short index) throws IOException {
    return new StripedBlockWriter(this, datanode, conf,
        reconstructor.getBlock(targetIndices[index]), targets[index],
        targetStorageTypes[index], targetStorageIds[index]);
  }

  ByteBuffer allocateWriteBuffer() {
    return reconstructor.allocateBuffer(reconstructor.getBufferSize());
  }

  int getTargets() {
    return targets.length;
  }

  private int getRealTargets() {
    int m = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        m++;
      }
    }
    return m;
  }

  int[] getRealTargetIndices() {
    int realTargets = getRealTargets();
    int[] results = new int[realTargets];
    int m = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        results[m++] = targetIndices[i];
      }
    }
    return results;
  }

  /**
   * 初始化target buffer，target buffer是准备写入到target的数据。
   * @param toReconstructLen internal block的长度
   * @return
   */
  ByteBuffer[] getRealTargetBuffers(int toReconstructLen) {
    int numGood = getRealTargets();
    ByteBuffer[] outputs = new ByteBuffer[numGood]; // 初始化状态下，position = 0
    int m = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) { // target is in good status
        writers[i].getTargetBuffer().limit(toReconstructLen);
        outputs[m++] = writers[i].getTargetBuffer();
      }
    }
    return outputs;
  }

  void updateRealTargetBuffers(int toReconstructLen) {
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        long blockLen = reconstructor.getBlockLen(targetIndices[i]);
        long remaining = blockLen - reconstructor.getPositionInBlock();
        if (remaining <= 0) {
          writers[i].getTargetBuffer().limit(0);
        } else if (remaining < toReconstructLen) {
          writers[i].getTargetBuffer().limit((int)remaining);
        }
      }
    }
  }

  byte[] getChecksumBuf() {
    return checksumBuf;
  }

  /**
   * 由于chunk就是checksum的基本单位，因此bytesPerChecksum就是一个chunk中的数据部分(不包括4byte的checksum)长度
   * @return
   */
  int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  int getChecksumSize() {
    return checksumSize;
  }

  DataChecksum getChecksum() {
    return reconstructor.getChecksum();
  }

  int getMaxChunksPerPacket() {
    return maxChunksPerPacket;
  }

  CachingStrategy getCachingStrategy() {
    return reconstructor.getCachingStrategy();
  }

  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo target) {
    return reconstructor.getSocketAddress4Transfer(target);
  }

  StripedReconstructor getReconstructor() {
    return reconstructor;
  }

  boolean hasValidTargets() {
    return hasValidTargets;
  }

  /**
   * Clear all buffers.
   */
  void clearBuffers() {
    for (StripedBlockWriter writer : writers) {
      ByteBuffer targetBuffer =
          writer != null ? writer.getTargetBuffer() : null;
      if (targetBuffer != null) {
        targetBuffer.clear();
      }
    }
  }

  void close() {
    for (StripedBlockWriter writer : writers) {
      ByteBuffer targetBuffer =
          writer != null ? writer.getTargetBuffer() : null;
      if (targetBuffer != null) {
        reconstructor.freeBuffer(targetBuffer);
        writer.freeTargetBuffer();
      }
    }

    for (int i = 0; i < targets.length; i++) {
      if (writers[i] != null) {
        writers[i].close();
      }
    }
  }
}
