package com.github.zk1931.pulsed;

/**
 * File Node.
 */
public class FileNode extends Node {

  final byte[] data;

  public FileNode(String fullPath,
                  long version,
                  long sessionID,
                  byte[] data) {
    super(fullPath, version, sessionID);
    if (data == null) {
      this.data = new byte[0];
    } else {
      this.data = data.clone();
    }
  }

  @Override
  public boolean isDirectory() {
    return false;
  }
}
