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

package com.github.zk1931.pulsefs;

import com.github.zk1931.pulsefs.tree.DataTree.DirectoryNode;
import com.github.zk1931.pulsefs.tree.DataTree.InvalidPath;
import com.github.zk1931.pulsefs.tree.DataTree.NodeAlreadyExist;
import com.github.zk1931.pulsefs.tree.DataTree.NotDirectory;
import com.github.zk1931.pulsefs.tree.DataTree.PathNotExist;
import com.github.zk1931.pulsefs.tree.DataTree.TreeException;
import com.github.zk1931.pulsefs.tree.DataTree;
import com.github.zk1931.pulsefs.tree.DirNode;
import com.github.zk1931.pulsefs.tree.Node;
import com.github.zk1931.pulsefs.tree.PathUtils;
import java.nio.charset.Charset;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;

/**
 * Command for creating a new session.
 */
public class CreateSessionCommand extends Command {

  private static final long serialVersionUID = 0L;
  final String manager;

  public CreateSessionCommand(String manager) {
    this.manager = manager;
  }

  Node execute(PulseFS pulsefs)
      throws PathNotExist, InvalidPath, DirectoryNode, NotDirectory,
             NodeAlreadyExist {
    DataTree tree = pulsefs.getTree();
    String dirPath = PulseFSConfig.PULSEFS_SESSIONS_PATH;
    DirNode sessionNode = (DirNode)tree.getNode(dirPath);
    // Use the version of /pulsefs/sessions directory as session ID.
    long sessionID = sessionNode.version;
    String fileName = String.format("%016d", sessionID);
    String path = PathUtils.concat(dirPath, fileName);
    // Itself it's also a session file.
    Node node =
      tree.createSessionFile(path,
                             manager.getBytes(Charset.forName("UTF-8")),
                             sessionID,
                             false,
                             false);
    if (manager.equals(pulsefs.getServerId())) {
      pulsefs.manageSession(sessionID);
    } else {
      pulsefs.abandonSession(sessionID);
    }
    return node;
  }

  void executeAndReply(PulseFS pulsefs, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      Node node = execute(pulsefs);
      // Since user has not idea of the path of newly craeted sequential node,
      // we need return it to user.
      response.addHeader("Location", node.fullPath);
      Utils.setHeader(node, response);
      Utils.replyCreated(response, context);
    } catch (PathNotExist ex) {
      Utils.replyNotFound(response, ex.getMessage(), context);
    } catch (TreeException ex) {
      Utils.replyBadRequest(response, ex.getMessage(), context);
    }
  }
}
