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

import com.github.zk1931.pulsefs.tree.DataTree;
import com.github.zk1931.pulsefs.tree.DataTree.DirectoryNode;
import com.github.zk1931.pulsefs.tree.DataTree.InvalidPath;
import com.github.zk1931.pulsefs.tree.DataTree.NodeAlreadyExist;
import com.github.zk1931.pulsefs.tree.DataTree.NotDirectory;
import com.github.zk1931.pulsefs.tree.DataTree.PathNotExist;
import com.github.zk1931.pulsefs.tree.DataTree.TreeException;
import com.github.zk1931.pulsefs.tree.DataTree.VersionNotMatch;
import com.github.zk1931.pulsefs.tree.Node;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for put, if file exists then update data of file, otherwise
 * creates a new file.
 */
public class PutCommand extends Command {

  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);

  final String path;
  final byte[] data;
  final boolean recursive;
  final long version;
  final boolean isTransient;
  final boolean ephemeral;
  final long sessionID;

  public PutCommand(String path,
                    byte[] data,
                    boolean recursive,
                    long version,
                    boolean isTransient,
                    boolean ephemeral,
                    long sessionID) {
    this.path = path;
    this.data = data.clone();
    this.recursive = recursive;
    this.version = version;
    this.isTransient = isTransient;
    this.ephemeral = ephemeral;
    this.sessionID = sessionID;
  }

  Node execute(PulseFS pulsefs)
      throws PathNotExist, InvalidPath, VersionNotMatch, DirectoryNode,
             NotDirectory, NodeAlreadyExist {
    DataTree tree = pulsefs.getTree();
    if (version < -1) {
      // If version is less than -1 then we do creation or set depends on if the
      // path exists in the tree or not.
      if (tree.exist(path)) {
        // If the node exists, treat the command as request of update.
        return tree.setData(path, data, -1);
      } else {
        if (ephemeral) {
          // Creates ephemeral file.
          return tree.createSessionFile(path,
                                        data,
                                        sessionID,
                                        recursive,
                                        isTransient);
        } else {
          // Otherwise treat the command as request of creation.
          return tree.createFile(path, data, recursive, isTransient);
        }
      }
    } else if (version == -1) {
      // If the version is -1 then we can only do creation.
      return tree.createFile(path, data, recursive, isTransient);
    } else {
      return tree.setData(path, data, version);
    }
  }

  void executeAndReply(PulseFS pulsefs, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      Node node = execute(pulsefs);
      Utils.setHeader(node, response);
      if (node.version == 0) {
        Utils.replyCreated(response, context);
      } else {
        Utils.replyOK(response, context);
      }
    } catch (PathNotExist ex) {
      Utils.replyNotFound(response, ex.getMessage(), context);
    } catch (VersionNotMatch ex) {
      Utils.replyConflict(response, ex.getMessage(), context);
    } catch (TreeException ex) {
      Utils.replyBadRequest(response, ex.getMessage(), context);
    }
  }
}
