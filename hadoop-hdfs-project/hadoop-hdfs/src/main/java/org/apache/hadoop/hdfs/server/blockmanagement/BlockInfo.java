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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

/**
 * For a given block (or an erasure coding block group), BlockInfo class
 * maintains 1) the {@link BlockCollection} it is part of, and 2) datanodes
 * where the replicas of the block, or blocks belonging to the erasure coding
 * block group, are stored.
 * 有了ec以后，BlockInfo的概念就泛化了。在continuous的场景下，一个BlockInfo代表的是一个block， triplets中的每一个三元组就代表了这个block的当前replica、上一个replica和下一个replica的信息
 * 而在striped场景下，BlockInfo就代表了一个group， triplets中的每一个三元组（尽管是平铺的）就代表了这个cell的上一个cell、当前cell和下一个cell
 */
@InterfaceAudience.Private
public abstract class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {

  public static final BlockInfo[] EMPTY_ARRAY = {};

  /**
   * Replication factor.
   */
  private short replication;

  /**
   * Block collection ID.
   */
  private volatile long bcId; // 这个block所属于的block collection，比如一个INodeFile就是一个BlockCollection

  /** For implementing {@link LightWeightGSet.LinkedElement} interface. */
  private LightWeightGSet.LinkedElement nextLinkedElement;

  /**
   * This array contains triplets of references. For each i-th storage, the
   * block belongs to triplets[3*i] is the reference to the
   * {@link DatanodeStorageInfo} and triplets[3*i+1] and triplets[3*i+2] are
   * references to the previous and the next blocks, respectively, in the list
   * of blocks belonging to this storage.
   *
   * Using previous and next in Object triplets is done instead of a
   * {@link LinkedList} list to efficiently use memory. With LinkedList the cost
   * per replica is 42 bytes (LinkedList#Entry object per replica) versus 16
   * bytes using the triplets.
   */
  protected Object[] triplets;

  private BlockUnderConstructionFeature uc;

  /**
   * Construct an entry for blocksmap
   * @param size the block's replication factor, or the total number of blocks
   *             in the block group
   */
  public BlockInfo(short size) {
    this.triplets = new Object[3 * size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size; //replication对于stripped是无效信息
  }

  public BlockInfo(Block blk, short size) {
    super(blk);
    this.triplets = new Object[3*size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size; // 如果是纠删吗，那么replication是0，否则就是size
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short repl) {
    this.replication = repl;
  }

  public long getBlockCollectionId() {
    return bcId;
  }

  public void setBlockCollectionId(long id) {
    this.bcId = id;
  }

  public void delete() {
    setBlockCollectionId(INVALID_INODE_ID);
  }

  public boolean isDeleted() {
    return bcId == INVALID_INODE_ID;
  }

  public Iterator<DatanodeStorageInfo> getStorageInfos() {
    return new BlocksMap.StorageIterator(this);
  }

  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index*3];
  }

  BlockInfo getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+1];
    assert info == null ||
        info.getClass().getName().startsWith(BlockInfo.class.getName()) :
        "BlockInfo is expected at " + index*3;
    return info;
  }

  BlockInfo getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfo.class.getName()) :
        "BlockInfo is expected at " + index*3;
    return info;
  }

  void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    triplets[index*3] = storage;
  }

  /**
   * Return the previous block on the block list for the datanode at
   * position index. Set the previous block on the list to "to".
   *
   * @param index - the datanode index
   * @param to - block to be set to previous on the list of blocks
   * @return current previous block on the list of blocks
   */
  BlockInfo setPrevious(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo) triplets[index*3+1];
    triplets[index*3+1] = to;
    return info;
  }

  /**
   * Return the next block on the block list for the datanode at
   * position index. Set the next block on the list to "to".
   *
   * @param index - the datanode index
   * @param to - block to be set to next on the list of blocks
   * @return current next block on the list of blocks
   */
  BlockInfo setNext(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo) triplets[index*3+2];
    triplets[index*3+2] = to;
    return info;
  }

  /**
   * 这里capacity返回的是triplets.length / 3， 即这个block的总的副本书，即这个block group中physical block的大小而不是这个triplets的长度
   * @return
   */
  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  /**
   * Count the number of data-nodes the block currently belongs to (i.e., NN
   * has received block reports from the DN).
   */
  public abstract int numNodes();

  /**
   * Add a {@link DatanodeStorageInfo} location for a block
   * @param storage The storage to add
   * @param reportedBlock The block reported from the datanode. This is only
   *                      used by erasure coded blocks, this block's id contains
   *                      information indicating the index of the block in the
   *                      corresponding block group.
   */
  abstract boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  /**
   * Remove {@link DatanodeStorageInfo} location for a block
   */
  abstract boolean removeStorage(DatanodeStorageInfo storage);

  public abstract boolean isStriped();

  public abstract BlockType getBlockType();

  /** @return true if there is no datanode storage associated with the block */
  abstract boolean hasNoStorage();

  /**
   * Checks whether this block has a Provided replica.
   * @return true if this block has a replica on Provided storage.
   */
  abstract boolean isProvided();

  /**
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity();
    DatanodeStorageInfo providedStorageInfo = null;
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur != null) {
        if (cur.getStorageType() == StorageType.PROVIDED) {
          // if block resides on provided storage, only match the storage ids
          if (dn.getStorageInfo(cur.getStorageID()) != null) {
            // do not return here as we have to check the other
            // DatanodeStorageInfos for this block which could be local
            providedStorageInfo = cur;
          }
        } else if (cur.getDatanodeDescriptor() == dn) {
          return cur;
        }
      }
    }
    return providedStorageInfo;
  }

  /**
   * Find specified DatanodeStorageInfo.
   * @return index or -1 if not found.
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) { // 通过遍历的方式找到当前的storageInfo的index
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
    }
    return -1;
  }

  /**
   * Insert this block into the head of the list of blocks
   * related to the specified DatanodeStorageInfo.
   * If the head is null then form a new list.
   * @return current block as the new head of the list.
   *
   * 根据当前的这个this(block)所载的DataNodeBlockInfo所维护的BlockInfoContinuous链表的头节点head，把this(block)的pre和next插入进去
   */
  BlockInfo listInsert(BlockInfo head, DatanodeStorageInfo storage) {
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is already in the list and cannot be inserted.";
    this.setPrevious(dnIndex, null);
    this.setNext(dnIndex, head); // next节点就是head
    if (head != null) {
      head.setPrevious(head.findStorageInfo(storage), this);// 把head节点的pre设置为当前的BlockInfoContinous节点
    }
    return this;
  }

  /**
   * Remove this block from the list of blocks
   * related to the specified DatanodeStorageInfo.
   * If this block is the head of the list then return the next block as
   * the new head.
   * @return the new head of the list or null if the list becomes
   * empy after deletion.
   */
  BlockInfo listRemove(BlockInfo head, DatanodeStorageInfo storage) {
    if (head == null) {
      return null;
    }
    int dnIndex = this.findStorageInfo(storage);
    if (dnIndex < 0) { // this block is not on the data-node list
      return head;
    }

    BlockInfo next = this.getNext(dnIndex);
    BlockInfo prev = this.getPrevious(dnIndex);
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    if (prev != null) {
      prev.setNext(prev.findStorageInfo(storage), next);
    }
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    if (this == head) { // removing the head
      head = next;
    }
    return head;
  }

  /**
   * Remove this block from the list of blocks related to the specified
   * DatanodeDescriptor. Insert it into the head of the list of blocks.
   *
   * @return the new head of the list.
   */
  public BlockInfo moveBlockToHead(BlockInfo head, DatanodeStorageInfo storage,
      int curIndex, int headIndex) {
    if (head == this) {
      return this;
    }
    BlockInfo next = this.setNext(curIndex, head);
    BlockInfo prev = this.setPrevious(curIndex, null);

    head.setPrevious(headIndex, this);
    prev.setNext(prev.findStorageInfo(storage), next);
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    return this;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }

  /* UnderConstruction Feature related */

  public BlockUnderConstructionFeature getUnderConstructionFeature() {
    return uc;
  }

  public BlockUCState getBlockUCState() {
    return uc == null ? BlockUCState.COMPLETE : uc.getBlockUCState();
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  public boolean isUnderRecovery() {
    return getBlockUCState().equals(BlockUCState.UNDER_RECOVERY);
  }

  public final boolean isCompleteOrCommitted() {
    final BlockUCState state = getBlockUCState();
    return state.equals(BlockUCState.COMPLETE) ||
        state.equals(BlockUCState.COMMITTED);
  }

  /**
   * Add/Update the under construction feature.
   * 这个block可能是stripped, 也可能是continuous的block，都是这个方法
   * 这个方法调用完成，那么个blockInfo中的uc就包含了这个underConstruction的block的location信息
   */
  public void convertToBlockUnderConstruction(BlockUCState s,
      DatanodeStorageInfo[] targets) {
    if (isComplete()) { // 这个block已经处于COMPLETE的状态，即最后的稳定状态。在COMPLETE状态下，uc=null，这种情况可能发生在block从COMPLETE状态又回到uc的状态
      uc = new BlockUnderConstructionFeature(this, s, targets,
          this.getBlockType());
    } else {
      // the block is already under construction
      uc.setBlockUCState(s);
      uc.setExpectedLocations(this, targets, this.getBlockType());
    }
  }

  /**
   * Convert an under construction block to complete.
   */
  void convertToCompleteBlock() {
    assert getBlockUCState() != BlockUCState.COMPLETE :
        "Trying to convert a COMPLETE block";
    uc = null;
  }

  /**
   * Process the recorded replicas. When about to commit or finish the
   * pipeline recovery sort out bad replicas.
   * @param genStamp  The final generation stamp for the block.
   * @return staleReplica's List.
   */
  public List<ReplicaUnderConstruction> setGenerationStampAndVerifyReplicas(
      long genStamp) {
    Preconditions.checkState(uc != null && !isComplete());
    // Set the generation stamp for the block.
    setGenerationStamp(genStamp);

    return uc.getStaleReplicas(genStamp);
  }

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation
   * @return staleReplica's List.
   * @throws IOException if block ids are inconsistent.
   */
  List<ReplicaUnderConstruction> commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    }
    Preconditions.checkState(!isComplete());
    uc.commit();
    this.setNumBytes(block.getNumBytes());
    // Sort out invalid replicas.
    return setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }
}
