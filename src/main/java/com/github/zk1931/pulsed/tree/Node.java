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

package com.github.zk1931.pulsed.tree;

import java.io.Serializable;

/**
 * Node of the tree.
 */
public abstract class Node implements Serializable {
  private static final long serialVersionUID = 0L;

  /**
   * The full path of the node.
   */
  public final String fullPath;

  /**
   * The version of the node.
   */
  public final long version;

  public Node(String fullPath,
              long version) {
    this.fullPath = fullPath;
    this.version = version;
  }

  public abstract boolean isDirectory();

  public abstract long getChecksum();

  public abstract String getNodeName();

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Node node = (Node)obj;
    return this.fullPath.equals(node.fullPath);
  }

  @Override
  public int hashCode() {
    return this.fullPath.hashCode();
  }
}
