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

import com.github.zk1931.pulsefs.tree.Node;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

/**
 * Snapshot Command. Although this class inherits from Command, but it's not
 * serializable.
 */
public class SnapshotCommand extends Command {

  private static final long serialVersionUID = 0L;
  AsyncContext context;

  SnapshotCommand(AsyncContext context) {
    this.context = context;
  }

  Node execute(PulseFS pulsefs) {
    return null;
  }

  void executeAndReply(PulseFS pulsefs, Object ctx) {
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    response.setStatus(HttpServletResponse.SC_OK);
    context.complete();
  }

  private void writeObject(java.io.ObjectOutputStream stream)
      throws java.io.IOException {
    throw new java.io.NotSerializableException(getClass().getName());
  }

  private void readObject(java.io.ObjectInputStream stream)
      throws java.io.IOException, ClassNotFoundException {
    throw new java.io.NotSerializableException(getClass().getName());
  }
}
