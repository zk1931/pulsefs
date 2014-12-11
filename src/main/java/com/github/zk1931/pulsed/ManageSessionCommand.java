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

import java.nio.charset.Charset;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;

/**
 * Command for taking the management(ownership) of a session.
 */
public class ManageSessionCommand extends Command {

  private static final long serialVersionUID = 0L;

  final long sessionID;
  final String newOwner;

  public ManageSessionCommand(long sessionID, String newOwner) {
    this.sessionID = sessionID;
    this.newOwner = newOwner;
  }

  Node execute(Pulsed pulsed) throws DataTree.TreeException {
    DataTree tree = pulsed.getTree();
    String dirPath = PulsedConfig.PULSED_SESSIONS_PATH;
    String file = String.format("%016d", sessionID);
    String path = dirPath + PathUtils.SEP + file;
    Node node =
      tree.setData(path, newOwner.getBytes(Charset.forName("UTF-8")), -1);
    if (pulsed.getServerId().equals(newOwner)) {
      pulsed.manageSession(sessionID);
    } else {
      pulsed.abandonSession(sessionID);
    }
    return node;
  }

  void executeAndReply(Pulsed pulsed, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      Node node = execute(pulsed);
      Utils.setHeader(node, response);
      Utils.replyOK(response, context);
    } catch (DataTree.PathNotExist ex) {
      Utils.replyNotFound(response, ex.getMessage(), context);
    } catch (DataTree.TreeException ex) {
      Utils.replyBadRequest(response, ex.getMessage(), context);
    }
  }
}
