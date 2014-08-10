package com.github.zk1931.pulsed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a member in pulsed.
 */
public final class Member {
  private static final Logger LOG = LoggerFactory.getLogger(Member.class);
  final String name;
  final String owner;
  final boolean isActive;

  public Member(String name, String owner, boolean isActive) {
    this.name = name;
    this.owner = owner;
    this.isActive = isActive;
  }
}
