package com.github.zk1931.pulsed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a member in pulsed.
 */
public final class Member {
  private static final Logger LOG = LoggerFactory.getLogger(Member.class);
  final String owner;
  final boolean active;

  public Member(String owner, boolean active) {
    this.owner = owner;
    this.active = active;
  }

  public String toString() {
    return String.format("Member:owner=%s,active=%b", owner, active);
  }
}
