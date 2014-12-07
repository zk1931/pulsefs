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
import java.nio.charset.Charset;
import java.util.HashMap;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for processing the requests for Pulsed server configuration.
 */
public final class PulsedServersHandler extends HttpServlet {

  private static final long serialVersionUID = 0L;

  private static final Logger LOG =
      LoggerFactory.getLogger(PulsedServersHandler.class);

  private final Pulsed pd;

  PulsedServersHandler(Pulsed pd) {
    this.pd = pd;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    LOG.info("Get");
    HashMap<String, Object> resp = new HashMap<>();
    resp.put("leader", this.pd.getLeader());
    if (this.pd.isLeader()) {
      resp.put("active_members", this.pd.getActiveMembers());
      resp.put("cluster_members", this.pd.getClusterMembers());
    } else {
      resp.put("cluster_members", this.pd.getClusterMembers());
    }
    String content = Utils.toJson(resp);
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentLength(content.length());
    response.getOutputStream()
            .write(content.getBytes(Charset.forName("UTF-8")));
  }

  @Override
  protected void doDelete(HttpServletRequest request,
                          HttpServletResponse response)
      throws ServletException, IOException {
    String peerId = request.getPathInfo().substring(1);
    if (!this.pd.getClusterMembers().contains(peerId)) {
      Utils.replyBadRequest(response, peerId + " is not in cluster.");
    } else {
      AsyncContext context = request.startAsync(request, response);
      try {
        this.pd.removePeer(peerId, context);
      } catch (ZabException ex) {
        Utils.replyServiceUnavailable(response, context);
      }
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
