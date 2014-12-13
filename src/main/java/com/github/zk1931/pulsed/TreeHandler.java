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
import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for processing the requests for the tree structure.
 */
public class  TreeHandler extends HttpServlet {

  private static final long serialVersionUID = 0L;

  private static final Logger LOG =
      LoggerFactory.getLogger(TreeHandler.class);

  protected final Pulsed pd;

  TreeHandler(Pulsed pd) {
    this.pd = pd;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String path = request.getRequestURI();
    DataTree tree = this.pd.getTree();
    boolean recursive;
    boolean wait;
    long version = -1;
    try {
      // Parse the query parameters.
      recursive = request.getParameter("recursive") != null;
      wait = request.getParameter("wait") != null;
      if (wait) {
        version = Long.parseLong(request.getParameter("wait"));
      }
    } catch (IllegalArgumentException ex) {
      Utils.replyBadRequest(response, ex.getMessage());
      return;
    }
    try {
      PathUtils.validatePath(path);
      if (wait) {
        // If the parameters contain "wait" then it's a watch request, instead
        // of serving it directly we need to flush it through Zab so all the
        // watch/Put requests will be processed within single thread.
        //long version = Long.parseLong(options.get("wait"));
        AsyncContext context = getContext(request, response);
        processWatchRequest(context, tree, path, version, recursive);
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
    AsyncContext context = getContext(request, response);
    // Since the version -1 means creation, we use -2 as default(does
    // creation/set depends the existence of the path).
    long version = -2;
    boolean dir;
    boolean recursive;
    boolean isTransient;
    byte[] data = Utils.readData(request);
    try {
      // Parse the query parameters.
      recursive = request.getParameter("recursive") != null;
      dir = request.getParameter("dir") != null;
      isTransient = request.getParameter("transient") != null;
      if (request.getParameter("version") != null) {
        version = Long.parseLong(request.getParameter("version"));
      }
    } catch (IllegalArgumentException ex) {
      Utils.replyBadRequest(response, ex.getMessage());
      return;
    }
    try {
      Command cmd;
      if (dir) {
        // Means it's a directory.
        cmd = new CreateDirCommand(path, recursive);
      } else {
        cmd = new PutCommand(path, data, recursive, version, isTransient);
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
    AsyncContext context = getContext(request, response);
    boolean recursive;
    long version = -1;
    try {
      // Parse the query parameters.
      recursive = request.getParameter("recursive") != null;
      if (request.getParameter("version") != null) {
        version = Long.parseLong(request.getParameter("version"));
      }
    } catch (IllegalArgumentException ex) {
      Utils.replyBadRequest(response, ex.getMessage());
      return;
    }
    try {
      Command cmd = new DeleteCommand(path, recursive, version);
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
    AsyncContext context = getContext(request, response);
    byte[] data = Utils.readData(request);
    boolean recursive = request.getParameter("recursive") != null;
    try {
      Command cmd = new CreateSeqFileCommand(path, data, recursive);
      this.pd.proposeStateChange(cmd, context);
    } catch (ZabException ex) {
      Utils.replyServiceUnavailable(response, context);
    }
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    super.service(req, resp);
    if (!req.isAsyncStarted()) {
      resp.addHeader("root-version", Long.toString(pd.getTree().rootVersion()));
    }
  }


  void processWatchRequest(AsyncContext ctx,
                           DataTree tree,
                           String path,
                           long version,
                           boolean recursive) {
    HttpServletResponse response = (HttpServletResponse)(ctx.getResponse());
    HttpWatch watch = new HttpWatch(version, recursive, path, ctx);
    Node node;
    // Since other threads might be modifying the tree or adding watches,
    // we need lock the whole tree.
    synchronized(tree) {
      try {
        try {
          node = tree.getNode(path);
        } catch (DataTree.PathNotExist ex) {
          node = null;
          if (version != 0) {
            Utils.replyNotFound(response, ex.getMessage(), ctx);
          } else {
            tree.addWatch(watch);
          }
        }
        if (node != null) {
          if (watch.isTriggerable(node)) {
            // The watch is for the version and it's triggerable now, triggers
            // it directly.
            watch.trigger(node);
          } else {
            tree.addWatch(watch);
          }
        }
      } catch (DataTree.TreeException ex) {
        Utils.replyBadRequest(response, ex.getMessage(), ctx);
      }
    }
  }

  protected AsyncContext getContext(HttpServletRequest request,
                                    HttpServletResponse response) {
    // This listener is responsible for adding global version number to
    // asynchronous request.
    AsyncListener listener = new AsyncListener() {
      @Override
      public void onComplete(AsyncEvent event) {
        String rootVersion = Long.toString(pd.getTree().rootVersion());
        ServletResponse resp = event.getSuppliedResponse();
        ((HttpServletResponse)resp).addHeader("root-version", rootVersion);
      }
      @Override
      public void onError(AsyncEvent event) {}
      @Override
      public void onStartAsync(AsyncEvent event) {}
      @Override
      public void onTimeout(AsyncEvent event) {}
    };
    AsyncContext context = request.startAsync(request, response);
    context.addListener(listener);
    // No timeout.
    context.setTimeout(0);
    return context;
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
