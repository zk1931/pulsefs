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

package com.github.zk1931.pulsefs.tree;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages all the watches of DataTree.
 */
public class WatchManager {

  private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);

  HashMap<String, List<Watch>> watches =
    new HashMap<String, List<Watch>>();

  /**
   * Adds a watch.
   *
   * @param the watch.
   */
  public void addWatch(Watch watch) {
    List<Watch> watchList = watches.get(watch.getPath());
    if (watchList == null) {
      watchList = new LinkedList<Watch>();
      watches.put(watch.getPath(), watchList);
    }
    watchList.add(watch);
  }

  /**
   * Triggers and removes all the triggerable watch of the given node.
   *
   * @param node the node on whom watches are monitoring.
   */
  public void triggerAndRemoveWatches(Node node) {
    List<Watch> watchList = watches.get(node.fullPath);
    if (watchList != null) {
      Iterator<Watch> iter = watchList.iterator();
      while (iter.hasNext()) {
        Watch watch = iter.next();
        if (watch.isTriggerable(node)) {
          watch.trigger(node);
          iter.remove();
        }
      }
      if (watchList.isEmpty()) {
        watches.remove(node.fullPath);
      }
    }
  }
}
