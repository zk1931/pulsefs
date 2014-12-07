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

import com.github.zk1931.pulsed.DataTree.PathNotExist;
import com.github.zk1931.pulsed.DataTree.TreeException;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for watching specific node.
 */
public class WatchCommand extends Command {

  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(WatchCommand.class);

  final String path;
  final long version;
  final boolean recursive;

  public WatchCommand(String path, long version, boolean recursive) {
    this.path = path;
    this.version = version;
    this.recursive = recursive;
  }

  void execute(DataTree tree) {
  }

  void executeAndReply(DataTree tree, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    HttpWatch watch = new HttpWatch(this, context);
    Node node;
    try {
      try {
        node = tree.getNode(this.path);
      } catch (PathNotExist ex) {
        node = null;
      }
      if (node == null && version == -1) {
        // The node doesn't exist and the watch is for the deletion of the
        // node, replies it directly.
        Utils.replyOK(response, context);
      } else if (node != null && watch.isTriggerable(node)) {
        // The watch is for the version and it's triggerable now, triggers  it
        // directly.
        watch.trigger(node);
      } else {
        // Otherwise add the watch to DataTree.
        tree.addWatch(watch);
      }
    } catch (TreeException ex) {
      Utils.replyBadRequest(response, ex.getMessage(), context);
    }
  }
}
