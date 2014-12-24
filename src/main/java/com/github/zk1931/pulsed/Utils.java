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

import com.github.zk1931.pulsed.tree.DirNode;
import com.github.zk1931.pulsed.tree.FileNode;
import com.github.zk1931.pulsed.tree.Node;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import java.io.DataInputStream;
import java.io.IOException;
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

  private Utils() {}

  public static byte[] readData(HttpServletRequest request) throws IOException {
    int length = request.getContentLength();
    byte[] data;
    if (length >= 0) {
      data = new byte[length];
      new DataInputStream(request.getInputStream()).readFully(data);
    } else {
      data = new byte[0];
    }
    return data;
  }

  public static String toJson(Object obj) {
    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder.setPrettyPrinting().create();
    return gson.toJson(obj);
  }

  public static void setHeader(Node node, HttpServletResponse response) {
    response.addHeader("version", Long.toString(node.version));
    response.addHeader("type", node.getNodeName());
    response.addHeader("checksum", String.format("%08X", node.getChecksum()));
  }

  public static void replyBadRequest(HttpServletResponse response,
                                     String desc) {
    replyBadRequest(response, desc, null);
  }

  public static void replyBadRequest(HttpServletResponse response,
                                     String desc,
                                     AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST, desc);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyOK(HttpServletResponse response) {
    replyOK(response, null);
  }

  public static void replyOK(HttpServletResponse response,
                             AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_OK);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyCreated(HttpServletResponse response) {
    replyCreated(response, null);
  }

  public static void replyCreated(HttpServletResponse response,
                                  AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_CREATED);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyNotFound(HttpServletResponse response,
                                   String desc) {
    replyNotFound(response, desc, null);
  }

  public static void replyNotFound(HttpServletResponse response,
                                   String desc,
                                   AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_NOT_FOUND, desc);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyServiceUnavailable(HttpServletResponse response) {
    replyServiceUnavailable(response, null);
  }

  public static void replyServiceUnavailable(HttpServletResponse response,
                                             AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyConflict(HttpServletResponse response, String desc) {
    replyConflict(response, desc, null);
  }

  public static void replyConflict(HttpServletResponse response,
                                   String desc,
                                   AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_CONFLICT, desc);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyForbidden(HttpServletResponse response) {
    replyForbidden(response, null);
  }

  public static void replyForbidden(HttpServletResponse response,
                                    AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyTimeout(HttpServletResponse response, String desc) {
    replyTimeout(response, desc, null);
  }

  public static void replyTimeout(HttpServletResponse response,
                                  String desc,
                                  AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT, desc);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyPrecondFailed(HttpServletResponse response,
                                        String desc) {
    replyPrecondFailed(response, desc, null);
  }

  public static void replyPrecondFailed(HttpServletResponse response,
                                        String desc,
                                        AsyncContext ctx) {
    response.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED, desc);
    if (ctx != null) {
      ctx.complete();
    }
  }

  public static void replyNodeInfo(HttpServletResponse response,
                                   Node node,
                                   boolean recursive) throws IOException {
    replyNodeInfo(response, node, recursive, null);
  }

  public static void replyNodeInfo(HttpServletResponse response,
                                   Node node,
                                   boolean recursive,
                                   AsyncContext ctx) throws IOException {
    setHeader(node, response);
    if (node instanceof FileNode) {
      response.getOutputStream().write(((FileNode)node).data);
    } else {
      JsonWriter writer = new JsonWriter(response.getWriter());
      // 2-space indentation.
      writer.setIndent("  ");
      writeDir(node, writer, recursive);
    }
    replyOK(response, ctx);
  }

  static void writeMetadata(Node node, JsonWriter writer) throws IOException {
    writer.beginObject();
    writer.name("version").value(node.version);
    writer.name("path").value(node.fullPath);
    writer.name("type").value(node.getNodeName());
    writer.name("checksum").value(String.format("%08X", node.getChecksum()));
    writer.endObject();
  }

  static void writeDir(Node node, JsonWriter writer, boolean recursive)
      throws IOException {
    writer.beginObject();
    writer.name("version").value(node.version);
    writer.name("path").value(node.fullPath);
    writer.name("type").value(node.getNodeName());
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
}
