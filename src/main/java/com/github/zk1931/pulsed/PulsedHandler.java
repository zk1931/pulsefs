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
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for processing the requests for Pulsed server configuration.
 */
public final class PulsedHandler extends HttpServlet {

  private static final long serialVersionUID = 0L;

  private static final Logger LOG =
      LoggerFactory.getLogger(PulsedHandler.class);

  private final Pulsed pd;

  PulsedHandler(Pulsed pd) {
    this.pd = pd;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    LOG.info("Get");
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentLength(0);
  }

  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    LOG.info("Put");
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    response.setContentLength(0);
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
