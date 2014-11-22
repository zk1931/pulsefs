package com.github.zk1931.pulsed;

/**
 * Node of the tree.
 */
public abstract class Node {
  /**
   * The full path of the node.
   */
  final String fullPath;

  /**
   * The version of the node.
   */
  final long version;

  /**
   * The session ID of the owner of the node.
   */
  final long sessionID;

  public Node(String fullPath,
              long version,
              long sessionID) {
    this.fullPath = fullPath;
    this.version = version;
    this.sessionID = sessionID;
  }

  public abstract boolean isDirectory();

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Node node = (Node)obj;
    return this.fullPath.equals(node.fullPath);
  }

  @Override
  public int hashCode() {
    return this.fullPath.hashCode();
  }
}
