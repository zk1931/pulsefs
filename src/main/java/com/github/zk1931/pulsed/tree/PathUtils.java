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

/**
 * Utility functions for tree path.
 */
public final class PathUtils {

  public static final String ROOT_PATH = "/";
  public static final String SEP = "/";

  private PathUtils() {}

  public static String head(String path) {
    int sepIdx = path.indexOf(SEP);
    if (sepIdx == -1) {
      return path;
    }
    return path.substring(0, sepIdx);
  }

  public static String tail(String path) {
    int sepIdx = path.indexOf(SEP);
    if (sepIdx == -1) {
      return "";
    }
    return path.substring(sepIdx + 1);
  }

  public static String name(String path) {
    int sepIdx = path.lastIndexOf(SEP);
    if (sepIdx == -1) {
      return path;
    }
    return path.substring(sepIdx + 1);
  }

  public static String concat(String path1, String path2) {
    if (path1.equals(ROOT_PATH)) {
      return path1 + path2;
    }
    return path1 + SEP + path2;
  }

  public static String trimRoot(String path) {
    return path.substring(ROOT_PATH.length());
  }

  public static void validatePath(String path) throws DataTree.InvalidPath {
    if (!path.startsWith(ROOT_PATH)) {
      throw new DataTree.InvalidPath("Path must start with " + ROOT_PATH);
    }
    if (!path.equals(ROOT_PATH) && path.endsWith("/")) {
      throw new DataTree.InvalidPath("Path other than root cannot end with /");
    }
  }
}
