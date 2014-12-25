/**
 * Licensed to the zk9131 under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
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

package com.github.zk1931.pulsed;

import com.github.zk1931.pulsed.tree.DataTree;
import com.github.zk1931.pulsed.tree.DirNode;
import com.github.zk1931.pulsed.tree.Node;
import com.github.zk1931.pulsed.tree.PathUtils;
import java.util.HashSet;
import java.util.Set;

/**
 * Cluster change command.
 */
public class ClusterChangeCommand extends Command {

  private static final long serialVersionUID = 0L;
  final Set<String> clusterMembers;
  final Set<String> activeFollowers;
  final String leader;

  ClusterChangeCommand(Set<String> clusterMembers,
                       Set<String> activeFollowers,
                       String leader) {
    this.clusterMembers = new HashSet<>(clusterMembers);
    this.activeFollowers = new HashSet<>(activeFollowers);
    this.leader = leader;
  }

  Node execute(Pulsed pulsed) throws DataTree.TreeException {
    DataTree tree = pulsed.getTree();
    // Gets node /pulsed/servers
    Node serversNode = tree.getNode(PulsedConfig.PULSED_SERVERS_PATH);
    for (Node child : ((DirNode)serversNode).children.values()) {
      // Deletes all the nodes under /pulsed/servers
      tree.deleteNodeInStagingArea(child.fullPath, -1, false);
    }
    for (String server : clusterMembers) {
      // Creates new nodes.
      String path = PulsedConfig.PULSED_SERVERS_PATH + PathUtils.SEP + server;
      tree.createFileInStagingArea(path, new byte[0], false, false);
    }
    // Commit changes.
    tree.commitStagingChanges();
    return null;
  }

  void executeAndReply(Pulsed pulsed, Object ctx) {
  }
}
