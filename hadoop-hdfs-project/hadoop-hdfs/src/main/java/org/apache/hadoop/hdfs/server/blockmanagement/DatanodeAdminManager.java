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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages decommissioning and maintenance state for DataNodes. A background
 * monitor thread periodically checks the status of DataNodes that are
 * decommissioning or entering maintenance state.
 * <p>
 * A DataNode can be decommissioned in a few situations:
 * <ul>
 * <li>If a DN is dead, it is decommissioned immediately.</li>
 * <li>If a DN is alive, it is decommissioned after all of its blocks
 * are sufficiently replicated. Merely under-replicated blocks do not
 * block decommissioning as long as they are above a replication
 * threshold.</li>
 * </ul>
 * In the second case, the DataNode transitions to a DECOMMISSION_INPROGRESS
 * state and is tracked by the monitor thread. The monitor periodically scans
 * through the list of insufficiently replicated blocks on these DataNodes to
 * determine if they can be DECOMMISSIONED. The monitor also prunes this list
 * as blocks become replicated, so monitor scans will become more efficient
 * over time.
 * <p>
 * DECOMMISSION_INPROGRESS nodes that become dead do not progress to
 * DECOMMISSIONED until they become live again. This prevents potential
 * durability loss for singly-replicated blocks (see HDFS-6791).
 * <p>
 * DataNodes can also be put under maintenance state for any short duration
 * maintenance operations. Unlike decommissioning, blocks are not always
 * re-replicated for the DataNodes to enter maintenance state. When the
 * blocks are replicated at least dfs.namenode.maintenance.replication.min,
 * DataNodes transition to IN_MAINTENANCE state. Otherwise, just like
 * decommissioning, DataNodes transition to ENTERING_MAINTENANCE state and
 * wait for the blocks to be sufficiently replicated and then transition to
 * IN_MAINTENANCE state. The block replication factor is relaxed for a maximum
 * of maintenance expiry time. When DataNodes don't transition or join the
 * cluster back by expiry time, blocks are re-replicated just as in
 * decommissioning case as to avoid read or write performance degradation.
 * <p>
 * This class depends on the FSNamesystem lock for synchronization.
 */
@InterfaceAudience.Private
public class DatanodeAdminManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminManager.class);
  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final HeartbeatManager hbManager;
  private final ScheduledExecutorService executor;

  private DatanodeAdminMonitorInterface monitor = null;

  DatanodeAdminManager(final Namesystem namesystem,
      final BlockManager blockManager, final HeartbeatManager hbManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.hbManager = hbManager;

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("DatanodeAdminMonitor-%d")
            .setDaemon(true).build());
  }

  /**
   * Start the DataNode admin monitor thread.
   * @param conf
   */
  void activate(Configuration conf) {
    final int intervalSecs = (int) conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);
    checkArgument(intervalSecs >= 0, "Cannot set a negative " +
        "value for " + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY);

    int blocksPerInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

    final String deprecatedKey =
        "dfs.namenode.decommission.nodes.per.interval";
    final String strNodes = conf.get(deprecatedKey);
    if (strNodes != null) {
      LOG.warn("Deprecated configuration key {} will be ignored.",
          deprecatedKey);
      LOG.warn("Please update your configuration to use {} instead.",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);
    }

    checkArgument(blocksPerInterval > 0,
        "Must set a positive value for "
        + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);

    final int maxConcurrentTrackedNodes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
        DFSConfigKeys
            .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);
    checkArgument(maxConcurrentTrackedNodes >= 0, "Cannot set a negative " +
        "value for "
        + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES);

    Class cls = null;
    try {
      cls = conf.getClass(
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS,
          DatanodeAdminDefaultMonitor.class);
      monitor =
          (DatanodeAdminMonitorInterface)ReflectionUtils.newInstance(cls, conf);
      monitor.setBlockManager(blockManager);
      monitor.setNameSystem(namesystem);
      monitor.setDatanodeAdminManager(this);
    } catch (Exception e) {
      throw new RuntimeException("Unable to create the Decommission monitor " +
          "from "+cls, e);
    }
    executor.scheduleWithFixedDelay(monitor, intervalSecs, intervalSecs,
        TimeUnit.SECONDS);

    LOG.debug("Activating DatanodeAdminManager with interval {} seconds, " +
            "{} max blocks per interval, " +
            "{} max concurrently tracked nodes.", intervalSecs,
        blocksPerInterval, maxConcurrentTrackedNodes);
  }

  /**
   * Stop the admin monitor thread, waiting briefly for it to terminate.
   */
  void close() {
    executor.shutdownNow();
    try {
      executor.awaitTermination(3000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {}
  }

  /**
   * Start decommissioning the specified datanode.
   * @param node
   */
  @VisibleForTesting
  public void startDecommission(DatanodeDescriptor node) {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      // Update DN stats maintained by HeartbeatManager
      hbManager.startDecommission(node);
      // hbManager.startDecommission will set dead node to decommissioned.
      if (node.isDecommissionInProgress()) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          LOG.info("Starting decommission of {} {} with {} blocks",
              node, storage, storage.numBlocks());
        }
        node.getLeavingServiceStatus().setStartTime(monotonicNow());
        monitor.startTrackingNode(node);
      }
    } else {
      LOG.trace("startDecommission: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }

  /**
   * Stop decommissioning the specified datanode.
   * @param node
   */
  @VisibleForTesting
  public void stopDecommission(DatanodeDescriptor node) {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      // Update DN stats maintained by HeartbeatManager
      hbManager.stopDecommission(node);
      // extra redundancy blocks will be detected and processed when
      // the dead node comes back and send in its full block report.
      if (node.isAlive()) {
        blockManager.processExtraRedundancyBlocksOnInService(node);
      }
      // Remove from tracking in DatanodeAdminManager
      monitor.stopTrackingNode(node);
    } else {
      LOG.trace("stopDecommission: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }

  /**
   * Start maintenance of the specified datanode.
   * @param node
   */
  @VisibleForTesting
  public void startMaintenance(DatanodeDescriptor node,
      long maintenanceExpireTimeInMS) {
    // Even if the node is already in maintenance, we still need to adjust
    // the expiration time.
    node.setMaintenanceExpireTimeInMS(maintenanceExpireTimeInMS);
    if (!node.isMaintenance()) {
      // Update DN stats maintained by HeartbeatManager
      hbManager.startMaintenance(node);
      // hbManager.startMaintenance will set dead node to IN_MAINTENANCE.
      if (node.isEnteringMaintenance()) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          LOG.info("Starting maintenance of {} {} with {} blocks",
              node, storage, storage.numBlocks());
        }
        node.getLeavingServiceStatus().setStartTime(monotonicNow());
      }
      // Track the node regardless whether it is ENTERING_MAINTENANCE or
      // IN_MAINTENANCE to support maintenance expiration.
      monitor.startTrackingNode(node);
    } else {
      LOG.trace("startMaintenance: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }


  /**
   * Stop maintenance of the specified datanode.
   * @param node
   */
  @VisibleForTesting
  public void stopMaintenance(DatanodeDescriptor node) {
    if (node.isMaintenance()) {
      // Update DN stats maintained by HeartbeatManager
      hbManager.stopMaintenance(node);

      // extra redundancy blocks will be detected and processed when
      // the dead node comes back and send in its full block report.
      if (!node.isAlive()) {
        // The node became dead when it was in maintenance, at which point
        // the replicas weren't removed from block maps.
        // When the node leaves maintenance, the replicas should be removed
        // from the block maps to trigger the necessary replication to
        // maintain the safety property of "# of live replicas + maintenance
        // replicas" >= the expected redundancy.
        blockManager.removeBlocksAssociatedTo(node);
      } else {
        // Even though putting nodes in maintenance node doesn't cause live
        // replicas to match expected replication factor, it is still possible
        // to have over replicated when the node leaves maintenance node.
        // First scenario:
        // a. Node became dead when it is at AdminStates.NORMAL, thus
        //    block is replicated so that 3 replicas exist on other nodes.
        // b. Admins put the dead node into maintenance mode and then
        //    have the node rejoin the cluster.
        // c. Take the node out of maintenance mode.
        // Second scenario:
        // a. With replication factor 3, set one replica to maintenance node,
        //    thus block has 1 maintenance replica and 2 live replicas.
        // b. Change the replication factor to 2. The block will still have
        //    1 maintenance replica and 2 live replicas.
        // c. Take the node out of maintenance mode.
        blockManager.processExtraRedundancyBlocksOnInService(node);
      }

      // Remove from tracking in DatanodeAdminManager
      monitor.stopTrackingNode(node);
    } else {
      LOG.trace("stopMaintenance: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }

  protected void setDecommissioned(DatanodeDescriptor dn) {
    dn.setDecommissioned();
    LOG.info("Decommissioning complete for node {}", dn);
  }

  protected void setInMaintenance(DatanodeDescriptor dn) {
    dn.setInMaintenance();
    LOG.info("Node {} has entered maintenance mode.", dn);
  }

  /**
   * Checks whether a block is sufficiently replicated/stored for
   * DECOMMISSION_INPROGRESS or ENTERING_MAINTENANCE datanodes. For replicated
   * blocks or striped blocks, full-strength replication or storage is not
   * always necessary, hence "sufficient".
   * @return true if sufficient, else false.
   * 检查是否足够，足够要比完全满足副本要求稍微弱一些
   */
  protected boolean isSufficient(BlockInfo block, BlockCollection bc,
                               NumberReplicas numberReplicas,
                               boolean isDecommission,
                               boolean isMaintenance) {
    // 在不考虑pending的情况下，如果当前的live已经大于等于要求的副本数，那么副本就完全充足了，不需要考虑了
    if (blockManager.hasEnoughEffectiveReplicas(block, numberReplicas, 0)) {
      // Block has enough replica, skip
      LOG.trace("Block {} does not need replication.", block);
      return true;
    }
    // hasEnoughEffectiveReplicas()返回false，即，不考虑pending的replica的情况下我们发现副本数小于expected，因此需要进一步检查
    final int numExpected = blockManager.getExpectedLiveRedundancyNum(block,
        numberReplicas);
    final int numLive = numberReplicas.liveReplicas();

    // Block is under-replicated
    LOG.trace("Block {} numExpected={}, numLive={}", block, numExpected,
        numLive);
    // 处于decommision状态，并且存活的副本数 小于 期望 存活的副本数
    // 这里可以看到，对于decommission，如果要求3副本，存活3副本，并且不是正在进行uc的文件的最后一个副本，那么isSufficient返回false
    if (isDecommission && numExpected > numLive) {
      if (bc.isUnderConstruction() && block.equals(bc.getLastBlock())) {
        // Can decom a UC block as long as there will still be minReplicas
        if (blockManager.hasMinStorage(block, numLive)) { // 这个最后一个block，并且live的replica已经满足了最低副本要求
          LOG.trace("UC block {} sufficiently-replicated since numLive ({}) "
              + ">= minR ({})", block, numLive,
              blockManager.getMinStorageNum(block));
          return true; // 对于一个文件的最后一个副本，这个副本正在构造，那么只要这个副本满足最小副本数，就返回True
        } else {
          LOG.trace("UC block {} insufficiently-replicated since numLive "
              + "({}) < minR ({})", block, numLive,
              blockManager.getMinStorageNum(block)); // 返回false
        }
      } else {
        // 存活的副本数，小于期望存活的副本数，但是 大于等于(不小于) 系统的块的默认副本数，比如系统配置3副本，并且的确存活了3副本
        // Can decom a non-UC as long as the default replication is met
        if (numLive >= blockManager.getDefaultStorageNum(block)) {
          return true; // 存活副本数不小于默认副本数
        }
        // 如果存活副本数小于默认副本数，将返回false
      }
    }
    if (isMaintenance // 处于maintenance状态，并且numLive 不小于maintenance状态下的block数量
            // 一个datanode允许进入maintenance状态的最小live副本数，这个数量只能小于等于系统默认副本数（可以等于）
      && numLive >= blockManager.getMinReplicationToBeInMaintenance()) {
      return true;
    }
    return false;
  }

  protected void logBlockReplicationInfo(BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor srcNode, NumberReplicas num,
      Iterable<DatanodeStorageInfo> storages) {
    if (!NameNode.blockStateChangeLog.isInfoEnabled()) {
      return;
    }

    int curReplicas = num.liveReplicas();
    int curExpectedRedundancy = blockManager.getExpectedRedundancyNum(block);
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      nodeList.append(node).append(' ');
    }
    NameNode.blockStateChangeLog.info(
        "Block: " + block + ", Expected Replicas: "
        + curExpectedRedundancy + ", live replicas: " + curReplicas
        + ", corrupt replicas: " + num.corruptReplicas()
        + ", decommissioned replicas: " + num.decommissioned()
        + ", decommissioning replicas: " + num.decommissioning()
        + ", maintenance replicas: " + num.maintenanceReplicas()
        + ", live entering maintenance replicas: "
        + num.liveEnteringMaintenanceReplicas()
        + ", replicas on stale nodes: " + num.replicasOnStaleNodes()
        + ", readonly replicas: " + num.readOnlyReplicas()
        + ", excess replicas: " + num.excessReplicas()
        + ", Is Open File: " + bc.isUnderConstruction()
        + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
        + srcNode + ", Is current datanode decommissioning: "
        + srcNode.isDecommissionInProgress() +
        ", Is current datanode entering maintenance: "
        + srcNode.isEnteringMaintenance());
  }

  @VisibleForTesting
  public int getNumPendingNodes() {
    return monitor.getPendingNodeCount();
  }

  @VisibleForTesting
  public int getNumTrackedNodes() {
    return monitor.getTrackedNodeCount();
  }

  @VisibleForTesting
  public int getNumNodesChecked() {
    return monitor.getNumNodesChecked();
  }

  @VisibleForTesting
  public Queue<DatanodeDescriptor> getPendingNodes() {
    return monitor.getPendingNodes();
  }

  @VisibleForTesting
  void runMonitorForTest() throws ExecutionException, InterruptedException {
    executor.submit(monitor).get();
  }

}