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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions.
 */
public final class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private Utils() {}

  public static String toJson(Object obj) {
    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder.setPrettyPrinting().create();
    return gson.toJson(obj);
  }

  public static Map<String, Object> buildMetadata(Node node) {
    Map<String, Object> nodeInfo = new HashMap<String, Object>();
    nodeInfo.put("version", Long.toString(node.version));
    String type;
    if (node.isDirectory()) {
      type = "directory";
    } else {
      type = "file";
    }
    nodeInfo.put("type", type);
    nodeInfo.put("session", node.sessionID);
    nodeInfo.put("path", node.fullPath);
    return nodeInfo;
  }

  public static Map<String, Object> buildNode(Node node) {
    Map<String, Object> nodeInfo = buildMetadata(node);
    ArrayList<Map<String, Object>> children = new ArrayList<>();
    if (node instanceof DirNode) {
      for (Node child : ((DirNode)node).children.values()) {
        children.add(buildMetadata(child));
      }
      nodeInfo.put("children", children);
    } else {
      if (!(node instanceof FileNode)) {
        throw new RuntimeException("node must be FileNode here.");
      }
      nodeInfo.put("data", ((FileNode)node).data);
    }
    return nodeInfo;
  }

  public static void badRequest(HttpServletResponse response, String desc,
                                AsyncContext ctx) {
    try {
      response.setContentType("text/html");
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, desc);
    } catch (IOException ex) {
      LOG.warn("IOExcepion {}", ex);
    } finally {
      if (ctx != null) {
        ctx.complete();
      }
    }
  }

  public static void badRequest(HttpServletResponse response, String desc) {
    badRequest(response, desc, null);
  }

  public static void notFound(HttpServletResponse response, String desc) {
    notFound(response, desc, null);
  }

  public static void notFound(HttpServletResponse response, String desc,
                              AsyncContext ctx) {
    try {
      response.setContentType("text/html");
      response.sendError(HttpServletResponse.SC_NOT_FOUND, desc);
    } catch (IOException ex) {
      LOG.warn("IOExcepion {}", ex);
    } finally {
      if (ctx != null) {
        ctx.complete();
      }
    }
  }

  public static void serviceUnavailable(HttpServletResponse response,
                                        AsyncContext ctx) {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    response.setContentLength(0);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void serviceUnavailable(HttpServletResponse response) {
    serviceUnavailable(response, null);
  }

  public static void replyOK(HttpServletResponse response, byte[] data) {
    replyOK(response, data, null);
  }

  public static void replyOK(HttpServletResponse response, byte[] data,
                             AsyncContext ctx) {
    try {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      if (data != null) {
        response.getOutputStream().write(data);
        response.setContentLength(data.length);
      }
    } catch (IOException ex) {
      LOG.error("IOException ", ex);
    } finally {
      if (ctx != null) {
        ctx.complete();
      }
    }
  }

  public static Set<String> getQueryStrings(String query) {
    if (query == null) {
      return new HashSet<String>();
    }
    return new HashSet<String>(Arrays.asList(query.split("&")));
  }
}
