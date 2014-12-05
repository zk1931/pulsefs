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

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

/**
 * HTTP Watch.
 */
public class HttpWatch implements Watch {

  final WatchCommand cmd;
  final AsyncContext ctx;

  HttpWatch(WatchCommand cmd, AsyncContext ctx) {
    this.cmd = cmd;
    this.ctx = ctx;
  }

  @Override
  public void trigger(Node node) {
    if (!isTriggerable(node)) {
      throw new RuntimeException("Not triggerable by " + node.version);
    }
    HttpServletResponse response = (HttpServletResponse)(ctx.getResponse());
    try {
      Utils.writeHeader(node, response);
      if (node instanceof DirNode) {
        Utils.writeChildren(node, response, cmd.recursive);
      } else {
        Utils.writeData(node, response);
      }
      this.ctx.complete();
    } catch (IOException ex) {
      Utils.badRequest(response, ex.getMessage(), ctx);
    }
  }

  @Override
  public String getPath() {
    return this.cmd.path;
  }

  @Override
  public boolean isTriggerable(Node node) {
    if (cmd.version == -1 && node.version != -1) {
      return false;
    }
    if (node.version >= cmd.version) {
      return true;
    }
    return false;
  }
}
