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
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions.
 */
public final class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static final String DIR_TYPE = "dir";
  private static final String FILE_TYPE = "file";

  private Utils() {}

  public static AsyncContext getContext(HttpServletRequest request,
                                        HttpServletResponse response) {
    AsyncContext context = request.startAsync(request, response);
    // No timeout.
    context.setTimeout(0);
    return context;
  }

  public static String toJson(Object obj) {
    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder.setPrettyPrinting().create();
    return gson.toJson(obj);
  }

  public static void writeHeader(Node node, HttpServletResponse response) {
    writeHeader(node, response, null);
  }

  public static void writeHeader(Node node, HttpServletResponse response,
                                 AsyncContext context) {
    String type = FILE_TYPE;
    if (node instanceof DirNode) {
      type = DIR_TYPE;
    }
    response.addHeader("version", Long.toString(node.version));
    response.addHeader("type", type);
    response.addHeader("path", node.fullPath);
    response.addHeader("checksum", String.format("%08X", node.getChecksum()));
    response.setStatus(HttpServletResponse.SC_OK);
    if (context != null) {
      context.complete();
    }
  }

  public static void writeData(Node node,
                               HttpServletResponse response)
      throws IOException {
    writeData(node, response, null);
  }

  public static void writeData(Node node,
                               HttpServletResponse response,
                               AsyncContext context) throws IOException {
    if (!(node instanceof FileNode)) {
      throw new RuntimeException("Node must be a file");
    }
    byte[] data = ((FileNode)node).data;
    response.getOutputStream().write(data);
    if (context != null) {
      context.complete();
    }
  }

  public static void writeChildren(Node node,
                                   HttpServletResponse response,
                                   boolean recursive) throws IOException {
    writeChildren(node, response, recursive, null);
  }

  public static void writeChildren(Node node,
                                   HttpServletResponse response,
                                   boolean recursive,
                                   AsyncContext context) throws IOException {
    if (!(node instanceof DirNode)) {
      throw new RuntimeException("Node must be a directory");
    }
    JsonWriter writer = new JsonWriter(response.getWriter());
    // 2-space indentation.
    writer.setIndent("  ");
    try {
      writeDir(node, writer, recursive);
    } finally {
      writer.close();
    }
    if (context != null) {
      context.complete();
    }
  }

  static void writeMetadata(Node node, JsonWriter writer) throws IOException {
    String type = FILE_TYPE;
    if (node instanceof DirNode) {
      type = DIR_TYPE;
    }
    writer.beginObject();
    writer.name("version").value(node.version);
    writer.name("path").value(node.fullPath);
    writer.name("sessionID").value(node.sessionID);
    writer.name("type").value(type);
    writer.name("checksum").value(String.format("%08X", node.getChecksum()));
    writer.endObject();
  }

  static void writeDir(Node node, JsonWriter writer, boolean recursive)
      throws IOException {
    writer.beginObject();
    writer.name("version").value(node.version);
    writer.name("path").value(node.fullPath);
    writer.name("sessionID").value(node.sessionID);
    writer.name("type").value(DIR_TYPE);
    writer.name("checksum").value(String.format("%08X", node.getChecksum()));
    writer.name("children");
    writeChildren(node, writer, recursive);
    writer.endObject();
  }

  static void writeChildren(Node node, JsonWriter writer, boolean recursive)
      throws IOException {
    writer.beginArray();
    for (Node child : ((DirNode)node).children.values()) {
      if (recursive && child instanceof DirNode) {
        writeDir(child, writer, recursive);
      } else {
        writeMetadata(child, writer);
      }
    }
    writer.endArray();
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

  // Parse the query string and convert to key-value pairs.
  //
  // For example:
  //
  // if the query string is "wait=2&recursive" then it returns:
  //    KEY         VALUE
  //    -----------------
  //    wait         2
  //    recursive    null
  //    -----------------
  public static Map<String, String> getQueries(String queryString) {
    if (queryString == null) {
      return new HashMap<String, String>();
    }
    Map<String, String> queryMap = new HashMap<String, String>();
    List<String> queries = Arrays.asList(queryString.split("&"));
    for (String query : queries) {
      String key;
      String value;
      int idx = query.indexOf("=");
      if (idx == -1) {
        key = query;
        value = null;
      } else {
        key = query.substring(0, idx);
        if (query.length() > idx + 1) {
          value = query.substring(idx + 1, query.length());
        } else {
          value = null;
        }
      }
      queryMap.put(key, value);
    }
    return queryMap;
  }
}
