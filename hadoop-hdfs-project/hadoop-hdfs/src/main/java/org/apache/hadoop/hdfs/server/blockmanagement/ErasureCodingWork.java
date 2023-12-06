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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.Node;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 一个ErasureCodingWork负责一个block group的调度。
 * 注意和DataNode端的ErasureCodingWorker区分开
 */
class ErasureCodingWork extends BlockReconstructionWork {
  private final byte[] liveBlockIndicies;
  private final byte[] liveBusyBlockIndicies;
  private final String blockPoolId;

  public ErasureCodingWork(String blockPoolId,
                           BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,// srcNodes.size()和liveBlockIndicies.size()一样，并且liveBlockIndicies相同位置的值就是对应的internal block在group中的位置
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired, int priority,
      byte[] liveBlockIndicies, byte[] liveBusyBlockIndicies) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    this.blockPoolId = blockPoolId;
    this.liveBlockIndicies = liveBlockIndicies;
    this.liveBusyBlockIndicies = liveBusyBlockIndicies;
    LOG.debug("Creating an ErasureCodingWork to {} reconstruct ",
        block);
  }

  byte[] getLiveBlockIndicies() {
    return liveBlockIndicies;
  }

  @Override
  /**
   * 由于是block的重构，因此，在发出申请的时候，将当前block group已经存在的block的target放在excludedNodes
   */
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) { //将当前block group已经存在的block的target放在excludedNodes
    // TODO: new placement policy for EC considering multiple writers
    // BlockPlacementPolicyRackFaultTolerant.chooseTarget
    DatanodeStorageInfo[] chosenTargets = blockplacement.chooseTarget(
        getSrcPath(), getAdditionalReplRequired(), getSrcNodes()[0],
        getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
        storagePolicySuite.getPolicy(getStoragePolicyID()), null);
    setTargets(chosenTargets);
  }

  /**
   * @return true if the current source nodes cover all the internal blocks.
   * I.e., we only need to have more racks.
   */
  private boolean hasAllInternalBlocks() {
    final BlockInfoStriped block = (BlockInfoStriped) getBlock();
    if (liveBlockIndicies.length
        + liveBusyBlockIndicies.length < block.getRealTotalBlockNum()) {
      return false;
    }
    BitSet bitSet = new BitSet(block.getTotalBlockNum());
    for (byte index : liveBlockIndicies) {
      bitSet.set(index);
    }
    for (byte busyIndex: liveBusyBlockIndicies) {
      bitSet.set(busyIndex);
    }
    for (int i = 0; i < block.getRealDataBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    for (int i = block.getDataBlockNum(); i < block.getTotalBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * We have all the internal blocks but not enough racks. Thus we do not need
   * to do decoding but only simply make an extra copy of an internal block. In
   * this scenario, use this method to choose the source datanode for simple
   * replication.
   * @return The index of the source datanode.
   * 其实就是选择了一个DN最多的rack上的一个DN
   */
  private int chooseSource4SimpleReplication() {
    // map 的key是rack，value是这个rack上的datanode
    Map<String, List<Integer>> map = new HashMap<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      final String rack = getSrcNodes()[i].getNetworkLocation();
      List<Integer> dnList = map.get(rack);
      if (dnList == null) {
        dnList = new ArrayList<>();
        map.put(rack, dnList);
      }
      dnList.add(i);
    } // 统计每一个rack上的dn
    List<Integer> max = null;
    for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
      if (max == null || entry.getValue().size() > max.size()) {
        max = entry.getValue();
      }
    }
    assert max != null;
    return max.get(0); // 其实就是选择了一个host最多的rack上的一个DN
  }

  @Override
  void addTaskToDatanode(NumberReplicas numberReplicas) {
    final DatanodeStorageInfo[] targets = getTargets();
    assert targets.length > 0;
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();

    if (hasNotEnoughRack()) { // 这个任务仅仅是拷贝internal block到其它的rack，而不是重算缺失的replica
      // if we already have all the internal blocks, but not enough racks,
      // we only need to replicate one internal block to a new rack
      int sourceIndex = chooseSource4SimpleReplication(); // 挑选一个DN最多的rack，然后从这个rack上拿出来一个host
      createReplicationWork(sourceIndex, targets[0]); // 把这个数据拷贝到第一个targets上去
    } else if ((numberReplicas.decommissioning() > 0 ||
        numberReplicas.liveEnteringMaintenanceReplicas() > 0) &&
        hasAllInternalBlocks()) { // 包括了decommissioning和 MAINTENANCE_FOR_READ的状态的internal block都没有丢，那么是不需要重算的，
      // 只需要把decommissioning 和 entering maintenance的节点上的internal block给复制出去
      // leavingServiceSources是那些由于处于decommission或者maintenance因此需要将对应的internal block从上面拷贝出去的节点
      List<Integer> leavingServiceSources = findLeavingServiceSources();
      // decommissioningSources.size() should be >= targets.length
      final int num = Math.min(leavingServiceSources.size(), targets.length);
      for (int i = 0; i < num; i++) { // 由于是encoding，正在离开service的节点上的internal block 必须复制走,这个复制走的任务是自己完成复制，即谁要DECOMMISSIONIGN了，谁负责把自己的节点复制走
        createReplicationWork(leavingServiceSources.get(i), targets[i]);
      }
    } else {
      // 将ec解码的任务交给第一个target数组中的第一个target，这个target会负责从所有的source中拉取internal block，经过decode，然后把数据写入到target中去
      targets[0].getDatanodeDescriptor().addBlockToBeErasureCoded( // 这时候是否知道缺少哪个block？
          new ExtendedBlock(blockPoolId, stripedBlk), getSrcNodes(), targets,
          getLiveBlockIndicies(), stripedBlk.getErasureCodingPolicy());
    }
  }

  /**
   * 虽然是EC 编码，但是由于这个问题仅仅是副本副本有问题，并没有缺少哪个副本，或者，由于sourceIndex节点进入maintenance或者decommioning，因此需要将它对应的block
   * replicate到target节点上去
   * @param sourceIndex
   * @param target
   */
  private void createReplicationWork(int sourceIndex,
      DatanodeStorageInfo target) {
    // stripedBlk.getNumBytes()是整个BlockGroup中的数据部分的总的size
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    // 从这里可以看到，srcNodes和liveBlockIndicies是size相同并且相同索引位置是完全对应的
    final byte blockIndex = liveBlockIndicies[sourceIndex];
    final DatanodeDescriptor source = getSrcNodes()[sourceIndex];
    final long internBlkLen = StripedBlockUtil.getInternalBlockLength(
        stripedBlk.getNumBytes(), stripedBlk.getCellSize(), // stripedBlk.getNumBytes()是数据部分的size
        stripedBlk.getDataBlockNum(), blockIndex);
    final Block targetBlk = new Block(stripedBlk.getBlockId() + blockIndex,
        internBlkLen, stripedBlk.getGenerationStamp()); // 创建需要进行replica的internal block
    source.addBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target}); //  添加到DataNode的toBeReplicate的source block中去，可以看到，这个是push的模式
    LOG.debug("Add replication task from source {} to "
        + "target {} for EC block {}", source, target, targetBlk);
  }

  /**
   * 正在decommission的节点，或者正在进入maintenance(还没有完成maintenance的存活节点)，都是返回值，
   * 这种节点可以优先被用来选择source节点进行复制操作
   * @return
   */
  private List<Integer> findLeavingServiceSources() {
    // Mark the block in normal node.
    BlockInfoStriped block = (BlockInfoStriped)getBlock();
    BitSet bitSet = new BitSet(block.getRealTotalBlockNum());// data block number + parity block number
    for (int i = 0; i < getSrcNodes().length; i++) {
      if (getSrcNodes()[i].isInService()) {
        bitSet.set(liveBlockIndicies[i]);
      }
    }
    // If the block is on the node which is decommissioning or
    // entering_maintenance, and it doesn't exist on other normal nodes,
    // we just add the node into source list.
    List<Integer> srcIndices = new ArrayList<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      // 如果这个节点正在进行decommission，或者正在进入maintenance(还可读)并且节点还有心跳，
      // 并且这个节点不是in_service，那么就认为这个节点正在leaving service
      // 查看EnrasureCodingWork L157, 正在leaving service的节点，优先选择用来进行replication
      if ((getSrcNodes()[i].isDecommissionInProgress() ||
          (getSrcNodes()[i].isEnteringMaintenance() &&
          getSrcNodes()[i].isAlive())) &&
          !bitSet.get(liveBlockIndicies[i])) {
        srcIndices.add(i); // 正在decommission的节点，或者正在进入maintenance(还没有完成maintenance的存活节点)，都是候选对象
      }
    }
    return srcIndices;
  }
}
