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

package com.github.zk1931.pulsed.tree;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages all the session files of DataTree.
 */
public class SessionFileManager {

  private static final Logger LOG =
    LoggerFactory.getLogger(SessionFileManager.class);

  private final Map<Long, Set<String>> sessionFiles =
    new HashMap<Long, Set<String>>();

  public void addFileToSession(long sessionID, String path) {
    Set<String> files = this.sessionFiles.get(sessionID);
    if (files == null) {
      files = new HashSet<String>();
      this.sessionFiles.put(sessionID, files);
    }
    if (files.contains(path)) {
      LOG.error("{} exists in session {}", path, sessionID);
      throw new RuntimeException(path + " exists in session " + sessionID);
    }
    files.add(path);
  }

  public void removeFileFromSession(long sessionID, String path) {
    Set<String> files = this.sessionFiles.get(sessionID);
    if (files == null) {
      LOG.error("Session {} doesn't exist.", sessionID);
      throw new RuntimeException("Session " + sessionID + " doesn't exist");
    }
    if (!files.remove(path)) {
      LOG.error("Session {} doesn't have file {}", sessionID, path);
      throw new RuntimeException("Session " + sessionID + " has not " + path);
    }
    if (files.isEmpty()) {
      this.sessionFiles.remove(sessionID);
    }
  }

  /**
   * Gets paths of all the session of with the given sessionID.
   */
  public Set<String> getSessionFiles(long sessionID) {
    Set<String> files = this.sessionFiles.get(sessionID);
    if (files == null) {
      return null;
    }
    // Returns the copy.
    return new HashSet<String>(files);
  }
}
