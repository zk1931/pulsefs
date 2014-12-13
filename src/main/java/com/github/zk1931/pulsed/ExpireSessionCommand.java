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

/**
 * Command for expiring a session.
 */
public class ExpireSessionCommand extends Command {

  private static final long serialVersionUID = 0L;

  final long sessionID;

  public ExpireSessionCommand(long sessionID) {
    this.sessionID = sessionID;
  }

  Node execute(Pulsed pulsed) {
    DataTree tree = pulsed.getTree();
    tree.deleteSession(sessionID);
    return null;
  }

  void executeAndReply(Pulsed pulsed, Object ctx) {
  }
}
