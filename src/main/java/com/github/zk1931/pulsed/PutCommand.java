package com.github.zk1931.pulsed;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to create a new session.
 */
public final class PutCommand implements Command, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);
  private static final long serialVersionUID = 0L;

  final String member;
  final String owner;

  public PutCommand(String member, String owner) {
    this.member = member;
    this.owner = owner;
  }

  public void execute(Database db, String origin) {
    db.put(member, owner);
  }

  public String toString() {
    return String.format("Put command: member=%s, owner=%s", member, owner);
  }
}
