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

import com.github.zk1931.pulsed.tree.Node;
import com.github.zk1931.pulsed.tree.Watch;
import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

/**
 * HTTP Watch.
 */
public class HttpWatch implements Watch {

  final long version;
  final boolean recursive;
  final AsyncContext ctx;
  final String path;
  boolean isTriggered = false;

  HttpWatch(long version, boolean recursive, String path, AsyncContext ctx) {
    this.version = version;
    this.recursive = recursive;
    this.path = path;
    this.ctx = ctx;
  }

  @Override
  public void trigger(Node node) {
    if (!isTriggerable(node)) {
      throw new RuntimeException("Not triggerable by " + node.version);
    }
    if (this.isTriggered) {
      return;
    }
    HttpServletResponse response = (HttpServletResponse)(ctx.getResponse());
    if (node.version == -1) {
      // Node just gets deleted, reply NOT_FOUND.
      Utils.replyNotFound(response, "not found", ctx);
    } else {
      try {
        Utils.replyNodeInfo(response, node, recursive, ctx);
      } catch (IOException ex) {
        Utils.replyBadRequest(response, ex.getMessage(), ctx);
      }
    }
    this.isTriggered = true;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public boolean isTriggerable(Node node) {
    if (node.version == -1) {
      // Node gets deleted, we must trigger the watch.
      return true;
    }
    if (this.version == -1) {
      return false;
    }
    return node.version >= this.version;
  }
}
