package com.github.zk1931.pulsed;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to deactivate a session.
 */
public final class DeactivateCommand implements Command, Serializable {
  private static final Logger LOG =
      LoggerFactory.getLogger(DeactivateCommand.class);
  private static final long serialVersionUID = 0L;

  final String member;
  final String owner;

  public DeactivateCommand(String member, String owner) {
    this.member = member;
    this.owner = owner;
  }

  public void execute(Database db) {
    db.deactivate(member, owner);
  }

  public String toString() {
    return String.format("Deactivate command: member=%s, owner=%s",
                         member, owner);
  }
}
