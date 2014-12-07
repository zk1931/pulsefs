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

import com.github.zk1931.jzab.ZabException;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for processing the requests for the tree structure.
 */
public final class  TreeHandler extends HttpServlet {

  private static final long serialVersionUID = 0L;

  private static final Logger LOG =
      LoggerFactory.getLogger(TreeHandler.class);

  private final Pulsed pd;

  TreeHandler(Pulsed pd) {
    this.pd = pd;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getRequestURI();
    DataTree tree = this.pd.getTree();
    Map<String, String> options = Utils.getQueries(request.getQueryString());
    boolean recursive = false;
    if (options.containsKey("recursive")) {
      recursive = true;
    }
    try {
      PathUtils.validatePath(path);
      if (options.containsKey("wait")) {
        // If the parameters contain "wait" then it's a watch request, instead
        // of serving it directly we need to flush it through Zab so all the
        // watch/Put requests will be processed within single thread.
        long version = Long.parseLong(options.get("wait"));
        Command watch = new WatchCommand(path, version, recursive);
        AsyncContext context = Utils.getContext(request, response);
        try {
          this.pd.proposeFlushRequest(watch, context);
        } catch (ZabException ex) {
          Utils.replyServiceUnavailable(response, context);
        }
      } else {
        // If it's not watch request, serves it directly.
        Node node = tree.getNode(path);
        Utils.replyNodeInfo(response, node, recursive);
      }
    } catch (DataTree.InvalidPath | NumberFormatException ex) {
      Utils.replyBadRequest(response, ex.getMessage());
    } catch (DataTree.PathNotExist | DataTree.NotDirectory ex) {
      Utils.replyNotFound(response, ex.getMessage());
    }
  }

  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getRequestURI();
    Map<String, String> options = Utils.getQueries(request.getQueryString());
    AsyncContext context = Utils.getContext(request, response);
    boolean recursive = false;
    if (options.containsKey("recursive")) {
      recursive = true;
    }
    try {
      Command cmd;
      if (options.containsKey("dir")) {
        // Means it's a directory.
        cmd = new CreateDirCommand(path, recursive);
      } else {
        int length = request.getContentLength();
        byte[] value;
        if (length >= 0) {
          value = new byte[length];
          new DataInputStream(request.getInputStream()).readFully(value);
        } else {
          value = new byte[0];
        }
        cmd = new PutCommand(path, value, recursive);
      }
      this.pd.proposeStateChange(cmd, context);
    } catch (ZabException ex) {
      Utils.replyServiceUnavailable(response, context);
    }
  }

  @Override
  protected void doDelete(HttpServletRequest request,
                          HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getRequestURI();
    Map<String, String> options = Utils.getQueries(request.getQueryString());
    AsyncContext context = Utils.getContext(request, response);
    boolean recursive = false;
    if (options.containsKey("recursive")) {
      recursive = true;
    }
    try {
      Command cmd = new DeleteCommand(path, recursive);
      this.pd.proposeStateChange(cmd, context);
    } catch (ZabException ex) {
      Utils.replyServiceUnavailable(response, context);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getRequestURI();
    Map<String, String> options = Utils.getQueries(request.getQueryString());
    AsyncContext context = Utils.getContext(request, response);
    boolean recursive = false;
    if (options.containsKey("recursive")) {
      recursive = true;
    }
    try {
      int length = request.getContentLength();
      byte[] value;
      if (length >= 0) {
        value = new byte[length];
        new DataInputStream(request.getInputStream()).readFully(value);
      } else {
        value = new byte[0];
      }
      Command cmd = new CreateSeqFileCommand(path, value, recursive);
      this.pd.proposeStateChange(cmd, context);
    } catch (ZabException ex) {
      Utils.replyServiceUnavailable(response, context);
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
