package com.github.zk1931.pulsed;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to add a member to a group.
 */
public final class JoinCommand implements Command, Serializable {
  private static final Logger LOG =
      LoggerFactory.getLogger(DeactivateCommand.class);
  private static final long serialVersionUID = 0L;

  final String member;
  final String group;

  public JoinCommand(String member, String group) {
    this.member = member;
    this.group = group;
  }

  public void execute(Database db) {
    db.join(member, group);
  }

  public String toString() {
    return String.format("Join command: member=%s, group=%s",
                         member, group);
  }
}
