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

/**
 * Interface for watch. Watch can monitor given node in DataTree.
 */
public interface  Watch {

  /**
   * Triggers the watch.
   *
   * @param node the node the watch is monitoring.
   */
  void trigger(Node node);

  /**
   * The path of node the watch is monitoring.
   */
  String getPath();

  /**
   * Whether the state of the node can trigger the watch or not.
   *
   * @param node the node the watch is monitoring.
   * @return true if it's triggerable, otherwise false.
   */
  boolean isTriggerable(Node node);
}
