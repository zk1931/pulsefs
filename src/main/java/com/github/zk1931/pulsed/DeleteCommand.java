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

import com.github.zk1931.pulsed.DataTree.DeleteRootDir;
import com.github.zk1931.pulsed.DataTree.DirectoryNotEmpty;
import com.github.zk1931.pulsed.DataTree.InvalidPath;
import com.github.zk1931.pulsed.DataTree.NotDirectory;
import com.github.zk1931.pulsed.DataTree.PathNotExist;
import com.github.zk1931.pulsed.DataTree.TreeException;
import com.github.zk1931.pulsed.DataTree.VersionNotMatch;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for deleting directory.
 */
public class DeleteCommand extends Command {

  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);

  final String path;
  final boolean recursive;
  final long version;

  public DeleteCommand(String path, boolean recursive, long version) {
    this.path = path;
    this.recursive = recursive;
    this.version = version;
  }

  Node execute(Pulsed pulsed)
      throws NotDirectory, PathNotExist, InvalidPath, DeleteRootDir,
             DirectoryNotEmpty, VersionNotMatch {
    DataTree tree = pulsed.getTree();
    return tree.deleteNode(this.path, this.version, this.recursive);
  }

  void executeAndReply(Pulsed pulsed, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      Node node = execute(pulsed);
      Utils.setHeader(node, response);
      Utils.replyOK(response, context);
    } catch (PathNotExist ex) {
      Utils.replyNotFound(response, ex.getMessage(), context);
    } catch (VersionNotMatch ex) {
      Utils.replyConflict(response, ex.getMessage(), context);
    } catch (TreeException ex) {
      Utils.replyBadRequest(response, ex.getMessage(), context);
    }
  }
}
