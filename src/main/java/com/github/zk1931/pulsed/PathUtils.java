package com.github.zk1931.pulsed;

import com.github.zk1931.pulsed.DataTree.InvalidPath;

/**
 * Utility functions for tree path.
 */
public final class PathUtils {

  static final String ROOT_PATH = "/";
  static final String SEP = "/";

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

  public static void validatePath(String path) throws InvalidPath {
    if (!path.startsWith(ROOT_PATH)) {
      throw new InvalidPath("Path must start with " + ROOT_PATH);
    }
    if (!path.equals(ROOT_PATH) && path.endsWith("/")) {
      throw new InvalidPath("Path other than root must not end with /");
    }
  }
}
