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

import com.github.zk1931.jzab.ZabException;
import com.github.zk1931.pulsefs.tree.DataTree;
import com.github.zk1931.pulsefs.tree.FileNode;
import com.github.zk1931.pulsefs.tree.Node;
import com.github.zk1931.pulsefs.tree.PathUtils;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for session request.
 */
public class PulseFSSessionsHandler extends PulseFSHandler {

  private static final long serialVersionUID = 0L;

  private static final Logger LOG =
      LoggerFactory.getLogger(PulseFSSessionsHandler.class);

  PulseFSSessionsHandler(PulseFS fs) {
    super(fs);
  }

  @Override
  protected void doPut(HttpServletRequest request,
                       HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getRequestURI();
    DataTree tree = this.fs.getTree();
    try {
      PathUtils.validatePath(path);
      Node node = tree.getNode(path);
      long sessionID = Long.parseLong(PathUtils.name(node.fullPath));
      if (Arrays.equals(((FileNode)node).data,
                        fs.getServerId().getBytes(Charset.forName("UTF-8")))) {
        Utils.replyNodeInfo(response, node, false);
        this.fs.renewSession(sessionID);
      } else {
        // This server is not the manager of session, trying to take the
        // management of the session.
        LOG.debug("Trying to declare the ownership of session {}", sessionID);
        AsyncContext context = getContext(request, response);
        Command manage = new ManageSessionCommand(sessionID,
                                                  fs.getServerId());
        try {
          this.fs.proposeStateChange(manage, context);
        } catch (ZabException ex) {
          Utils.replyServiceUnavailable(response, context);
        }
      }
    } catch (DataTree.InvalidPath ex) {
      Utils.replyBadRequest(response, ex.getMessage());
    } catch (DataTree.PathNotExist | DataTree.NotDirectory ex) {
      Utils.replyNotFound(response, ex.getMessage());
    }
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getPathInfo();
    String fullPath = request.getRequestURI();
    if (!path.equals("/")) {
      // User can only create a session by POST request to /pulsefs/sessions
      Utils.replyForbidden(response);
      return;
    }
    try {
      PathUtils.validatePath(fullPath);
      AsyncContext context = getContext(request, response);
      // Creates a new session.
      Command create = new CreateSessionCommand(this.fs.getServerId());
      try {
        this.fs.proposeStateChange(create, context);
      } catch (ZabException ex) {
        Utils.replyServiceUnavailable(response, context);
      }
    } catch (DataTree.InvalidPath ex) {
      Utils.replyBadRequest(response, ex.getMessage());
    }
  }

  /**
   * "Disables" serializable.
   */
  private void writeObject(java.io.ObjectOutputStream stream)
      throws IOException {
    throw new java.io.NotSerializableException(getClass().getName());
  }

  /**
   * "Disables" serializable.
   */
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    throw new java.io.NotSerializableException(getClass().getName());
  }
}
