package com.github.zk1931.pulsed;

import java.util.Collections;
import java.util.Map;

/**
 * Directory Node.
 */
public class DirNode extends Node {

  final Map<String, Node> children;

  public DirNode(String fullPath,
                 long version,
                 long sessionID,
                 Map<String, Node> children) {
    super(fullPath, version, sessionID);
    this.children = Collections.unmodifiableMap(children);
  }

  @Override
  public boolean isDirectory() {
    return true;
  }
}
