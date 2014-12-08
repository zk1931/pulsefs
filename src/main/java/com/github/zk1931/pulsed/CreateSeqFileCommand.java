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

import com.github.zk1931.pulsed.DataTree.DirectoryNode;
import com.github.zk1931.pulsed.DataTree.InvalidPath;
import com.github.zk1931.pulsed.DataTree.NodeAlreadyExist;
import com.github.zk1931.pulsed.DataTree.NotDirectory;
import com.github.zk1931.pulsed.DataTree.PathNotExist;
import com.github.zk1931.pulsed.DataTree.TreeException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for creating sequential file.
 */
public class CreateSeqFileCommand extends Command {

  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);

  final String dirPath;
  final boolean recursive;
  final byte[] data;

  public CreateSeqFileCommand(String dirPath, byte[] data, boolean recursive) {
    this.dirPath = dirPath;
    this.recursive = recursive;
    this.data = data.clone();
  }

  Node execute(DataTree tree)
      throws PathNotExist, InvalidPath, DirectoryNode, NotDirectory,
             NodeAlreadyExist {
    Node node = tree.getNode(this.dirPath);
    if (!(node instanceof DirNode)) {
      throw new NotDirectory(node.fullPath + " is not directory.");
    }
    Map<String, Node> children = ((DirNode)node).children;
    long max = -1;
    for (Node child : children.values()) {
      // File format is like : 0000000001
      String name = PathUtils.name(child.fullPath);
      if (name.matches("\\d{19}")) {
        long id = Long.parseLong(name);
        if (id > max) {
          max = id;
        }
      }
    }
    long newID = max + 1;
    String fileName = String.format("%019d", newID);
    String path = PathUtils.concat(this.dirPath, fileName);
    return tree.createFile(path, this.data, -1, recursive);
  }

  void executeAndReply(DataTree tree, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      Node node = execute(tree);
      Utils.setHeader(node, response);
      Utils.replyCreated(response, context);
    } catch (PathNotExist ex) {
      Utils.replyNotFound(response, ex.getMessage(), context);
    } catch (TreeException ex) {
      Utils.replyBadRequest(response, ex.getMessage(), context);
    }
  }
}
