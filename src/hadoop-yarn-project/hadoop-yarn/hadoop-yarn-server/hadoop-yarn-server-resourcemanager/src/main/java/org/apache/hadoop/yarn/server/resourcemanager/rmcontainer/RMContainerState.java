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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

public enum RMContainerState {
  NEW,  // Container刚刚建立，还没有分配资源
  RESERVED,  // 为这个Container进行资源预留
  ALLOCATED,  // 已经为这个Container分配了节点
  ACQUIRED,  // ApplicationMaster已经获取了资源请求对应的分配节点的信息，剩下的就是ApplicationMaster拿着对应的NMToken通NodeManager进行通信从而启动container了
  RUNNING,  // 这个Container已经在对应的NodeManager上运行
  COMPLETED,  // 这个Conainer已经结束
  EXPIRED,  // 这个Container已经过期了
  RELEASED,  // 这个Container已经释放了
  KILLED // 这个Container被杀死了
}
